import org.apache.commons.lang3.NotImplementedException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatIni implements FlatService {

    private static final char META_SEP = '\u0000';
    private static final String META_PREFIX = "FI3:";
    private static final String LS = System.lineSeparator();

    private enum EntryType {
        SECTION, COMMENT, EMPTY, DATA
    }

    private static final class Entry {
        private final EntryType type;
        private final String a;

        private Entry(EntryType type, String a) {
            this.type = type;
            this.a = a;
        }

        static Entry section(String name) {
            return new Entry(EntryType.SECTION, name);
        }

        static Entry comment(String raw) {
            return new Entry(EntryType.COMMENT, raw);
        }

        static Entry empty() {
            return new Entry(EntryType.EMPTY, null);
        }

        static Entry data(String flatKey) {
            return new Entry(EntryType.DATA, flatKey);
        }
    }

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        LinkedHashMap<String, FileDataItem> result = new LinkedHashMap<>();
        if (data == null || data.trim().isEmpty()) {
            return result;
        }

        String normalized = normalizeNewlines(data);
        List<String> lines = splitLinesPreserveEmpty(normalized);

        boolean hasSections = false;
        for (String line : lines) {
            if (line != null) {
                String t = line.trim();
                if (isSectionHeader(t)) {
                    hasSections = true;
                    break;
                }
            }
        }

        List<Entry> structure = new ArrayList<>();
        if (hasSections) {
            parseSectioned(lines, result, structure);
        } else {
            parsePlain(lines, result, structure);
        }

        String meta = encodeMeta(structure);
        for (FileDataItem item : result.values()) {
            if (item == null) {
                continue;
            }
            String p = item.getPath();
            if (p == null) {
                p = item.getKey();
            }
            if (p == null) {
                continue;
            }
            item.setPath(p + META_SEP + meta);
        }

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        List<Entry> meta = tryDecodeMeta(data);
        if (meta != null) {
            return dumpUsingMeta(data, meta);
        }

        boolean anySectioned = false;
        for (String k : data.keySet()) {
            if (k != null && k.contains("[") && k.contains("]")) {
                anySectioned = true;
                break;
            }
        }

        if (anySectioned) {
            return dumpSectionedFallback(data);
        }

        return dumpPlainFallback(data);
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation of INI files is not implemented yet.");
    }

    private static void parsePlain(List<String> lines, LinkedHashMap<String, FileDataItem> result, List<Entry> structure) {
        List<String> pendingMeta = new ArrayList<>();

        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                structure.add(Entry.empty());
                pendingMeta.clear();
                continue;
            }

            if (isCommentLine(trimmed)) {
                structure.add(Entry.comment(line));
                String content = stripCommentPrefix(trimmed);
                if (!content.isEmpty() && isMetaComment(content)) {
                    pendingMeta.add(content);
                }
                continue;
            }

            int eq = line.indexOf('=');
            if (eq < 0) {
                continue;
            }

            String key = line.substring(0, eq).trim();
            String value = line.substring(eq + 1).trim();
            if (key.isEmpty()) {
                continue;
            }

            FileDataItem item = new FileDataItem();
            item.setKey(key);
            item.setPath(key);
            item.setValue(value);
            if (!pendingMeta.isEmpty()) {
                item.setComment(joinWithNewlines(pendingMeta));
            }
            pendingMeta.clear();

            result.put(key, item);
            structure.add(Entry.data(key));
        }
    }

    private static void parseSectioned(List<String> lines, LinkedHashMap<String, FileDataItem> result, List<Entry> structure) {
        String currentSection = null;
        boolean currentSectionHadData = false;
        int indexInSection = 0;

        List<String> pendingMeta = new ArrayList<>();

        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                structure.add(Entry.empty());
                pendingMeta.clear();
                continue;
            }

            if (isSectionHeader(trimmed)) {
                if (currentSection != null && !currentSectionHadData) {
                    ensureEmptySectionPlaceholder(result, currentSection);
                }

                currentSection = trimmed.substring(1, trimmed.length() - 1).trim();
                currentSectionHadData = false;
                indexInSection = 0;
                pendingMeta.clear();

                structure.add(Entry.section(currentSection));
                continue;
            }

            if (isCommentLine(trimmed)) {
                structure.add(Entry.comment(line));
                String content = stripCommentPrefix(trimmed);
                if (!content.isEmpty() && isMetaComment(content)) {
                    pendingMeta.add(content);
                }
                continue;
            }

            if (currentSection == null) {
                continue;
            }

            String flatKey;
            String value;

            String lineTrimmed = trimmed;

            boolean isChildrenSection = currentSection.contains(":children");
            boolean hasWs = containsWhitespace(lineTrimmed);

            if (isChildrenSection || !hasWs) {
                flatKey = currentSection + "[" + indexInSection + "]";
                value = lineTrimmed;
            } else {
                int ws = firstWhitespaceIndex(lineTrimmed);
                String firstToken = lineTrimmed.substring(0, ws);
                String rest = lineTrimmed.substring(ws).trim();
                if (rest.isEmpty()) {
                    flatKey = currentSection + "[" + indexInSection + "]";
                    value = firstToken;
                } else if (shouldUseDotKey(rest)) {
                    flatKey = currentSection + "[" + indexInSection + "]." + firstToken;
                    value = rest;
                } else {
                    flatKey = currentSection + "[" + indexInSection + "]";
                    value = lineTrimmed;
                }
            }

            FileDataItem item = new FileDataItem();
            item.setKey(flatKey);
            item.setPath(flatKey);
            item.setValue(value);
            if (!pendingMeta.isEmpty()) {
                item.setComment(joinWithNewlines(pendingMeta));
            }
            pendingMeta.clear();

            result.put(flatKey, item);
            structure.add(Entry.data(flatKey));

            currentSectionHadData = true;
            indexInSection++;
        }

        if (currentSection != null && !currentSectionHadData) {
            ensureEmptySectionPlaceholder(result, currentSection);
        }
    }

    private static String dumpUsingMeta(Map<String, FileDataItem> data, List<Entry> meta) {
        List<String> out = new ArrayList<>();
        boolean skippedData = false;

        for (Entry e : meta) {
            if (e.type == EntryType.SECTION) {
                out.add("[" + safe(e.a) + "]");
                skippedData = false;
                continue;
            }
            if (e.type == EntryType.COMMENT) {
                out.add(safe(e.a));
                skippedData = false;
                continue;
            }
            if (e.type == EntryType.EMPTY) {
                if (skippedData) {
                    if (!out.isEmpty() && out.get(out.size() - 1).isEmpty()) {
                        skippedData = false;
                        continue;
                    }
                }
                out.add("");
                skippedData = false;
                continue;
            }
            if (e.type == EntryType.DATA) {
                String flatKey = e.a;
                FileDataItem item = getByFlatKey(data, flatKey);
                if (item == null) {
                    skippedData = true;
                    continue;
                }
                String line = formatLineFromFlatKey(flatKey, item);
                out.add(line);
                skippedData = false;
            }
        }

        return joinLinesNoTrailingNewline(out);
    }

    private static String dumpPlainFallback(Map<String, FileDataItem> data) {
        List<String> keys = new ArrayList<>(data.keySet());
        keys.sort(String::compareTo);

        List<String> out = new ArrayList<>();
        for (String k : keys) {
            FileDataItem item = data.get(k);
            if (item == null || item.getKey() == null) {
                continue;
            }
            appendCommentLines(out, item.getComment());
            out.add(item.getKey() + "=" + safeValue(item));
        }
        return joinLinesNoTrailingNewline(out);
    }

    private static String dumpSectionedFallback(Map<String, FileDataItem> data) {
        LinkedHashMap<String, List<SectionItem>> bySection = new LinkedHashMap<>();

        for (Map.Entry<String, FileDataItem> e : data.entrySet()) {
            String flatKey = e.getKey();
            if (flatKey == null) {
                continue;
            }
            SectionKey sk = parseSectionKey(flatKey);
            if (sk == null) {
                continue;
            }
            bySection.computeIfAbsent(sk.section, x -> new ArrayList<>())
                    .add(new SectionItem(sk.section, sk.index, flatKey, e.getValue(), sk.hasId, sk.id));
        }

        List<String> out = new ArrayList<>();
        boolean firstSection = true;

        for (Map.Entry<String, List<SectionItem>> sec : bySection.entrySet()) {
            String section = sec.getKey();
            List<SectionItem> items = sec.getValue();

            items.sort((a, b) -> {
                if (a.index != b.index) {
                    return Integer.compare(a.index, b.index);
                }
                return a.flatKey.compareTo(b.flatKey);
            });

            if (!firstSection) {
                out.add("");
            }
            firstSection = false;

            out.add("[" + section + "]");

            for (SectionItem si : items) {
                if (si.item == null) {
                    continue;
                }
                appendCommentLines(out, si.item.getComment());
                out.add(formatLineFromSectionKey(si, si.item));
            }
        }

        return joinLinesNoTrailingNewline(out);
    }

    private static String formatLineFromFlatKey(String flatKey, FileDataItem item) {
        if (flatKey == null) {
            return safeValue(item);
        }
        int dot = flatKey.indexOf("].");
        if (dot >= 0) {
            String id = flatKey.substring(dot + 2);
            String v = safeValue(item);
            if (v.isEmpty()) {
                return id;
            }
            return id + " " + v;
        }
        return safeValue(item);
    }

    private static String formatLineFromSectionKey(SectionItem si, FileDataItem item) {
        String v = safeValue(item);
        if (si.hasId && si.id != null && !si.id.isEmpty()) {
            if (v.isEmpty()) {
                return si.id;
            }
            return si.id + " " + v;
        }
        return v;
    }

    private static void appendCommentLines(List<String> out, String comment) {
        if (comment == null) {
            return;
        }
        String t = comment.trim();
        if (t.isEmpty()) {
            return;
        }
        List<String> lines = splitLinesPreserveEmpty(normalizeNewlines(comment));
        for (String c : lines) {
            if (c == null) {
                continue;
            }
            String cc = c.trim();
            if (cc.isEmpty()) {
                continue;
            }
            out.add("#" + cc);
        }
    }

    private static FileDataItem getByFlatKey(Map<String, FileDataItem> data, String flatKey) {
        FileDataItem direct = data.get(flatKey);
        if (direct != null) {
            return direct;
        }
        for (FileDataItem v : data.values()) {
            if (v != null && flatKey != null && flatKey.equals(v.getKey())) {
                return v;
            }
        }
        return null;
    }

    private static String safeValue(FileDataItem item) {
        Object v = item == null ? null : item.getValue();
        return v == null ? "" : String.valueOf(v);
    }

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.length() >= 2;
    }

    private static boolean isCommentLine(String trimmed) {
        return trimmed.startsWith("#") || trimmed.startsWith(";");
    }

    private static String stripCommentPrefix(String trimmed) {
        String t = trimmed;
        if (t.startsWith("#") || t.startsWith(";")) {
            t = t.substring(1);
        }
        return t.trim();
    }

    private static boolean isMetaComment(String content) {
        if (content == null) {
            return false;
        }
        String c = content.trim();
        if (c.isEmpty()) {
            return false;
        }
        if (looksLikeCommentedOutDataLine(c)) {
            return false;
        }
        return true;
    }

    private static boolean looksLikeCommentedOutDataLine(String content) {
        String c = content.trim();
        if (c.isEmpty()) {
            return false;
        }
        if (c.indexOf('=') >= 0) {
            return true;
        }
        int ws = firstWhitespaceIndex(c);
        if (ws < 0) {
            return false;
        }
        String rest = c.substring(ws).trim();
        if (rest.contains("ansible_") || rest.contains("ansible-host") || rest.contains("ansibleHost")) {
            return true;
        }
        return rest.contains("ansible_host=") || rest.contains("ansible_user=") || rest.contains("ansible_connection=");
    }

    private static boolean shouldUseDotKey(String rest) {
        String r = rest.trim();
        if (r.isEmpty()) {
            return false;
        }
        if (r.contains("ansible_host=") || r.contains("ansible_user=") || r.contains("ansible_connection=") || r.contains("ansible_ssh_")) {
            return true;
        }
        return r.contains("ansible_");
    }

    private static boolean containsWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static int firstWhitespaceIndex(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static void ensureEmptySectionPlaceholder(LinkedHashMap<String, FileDataItem> result, String section) {
        String key = section + "[0]";
        if (result.containsKey(key)) {
            return;
        }
        FileDataItem item = new FileDataItem();
        item.setKey(key);
        item.setPath(key);
        item.setValue("");
        result.put(key, item);
    }

    private static String normalizeNewlines(String s) {
        if (s == null) {
            return "";
        }
        String r = s.replace("\r\n", "\n");
        r = r.replace("\r", "\n");
        return r;
    }

    private static List<String> splitLinesPreserveEmpty(String s) {
        List<String> out = new ArrayList<>();
        if (s == null || s.isEmpty()) {
            out.add("");
            return out;
        }
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\n') {
                out.add(s.substring(start, i));
                start = i + 1;
            }
        }
        out.add(s.substring(start));
        return out;
    }

    private static String joinLinesNoTrailingNewline(List<String> lines) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            if (i > 0) {
                sb.append(LS);
            }
            sb.append(lines.get(i) == null ? "" : lines.get(i));
        }
        return sb.toString();
    }

    private static String joinWithNewlines(List<String> parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(LS);
            }
            sb.append(parts.get(i));
        }
        return sb.toString();
    }

    private static String encodeMeta(List<Entry> structure) {
        StringBuilder sb = new StringBuilder();
        for (Entry e : structure) {
            if (e == null) {
                continue;
            }
            if (e.type == EntryType.SECTION) {
                sb.append("S|").append(escape(e.a)).append("\n");
            } else if (e.type == EntryType.COMMENT) {
                sb.append("C|").append(escape(e.a)).append("\n");
            } else if (e.type == EntryType.EMPTY) {
                sb.append("E|\n");
            } else if (e.type == EntryType.DATA) {
                sb.append("D|").append(escape(e.a)).append("\n");
            }
        }
        String raw = sb.toString();
        String b64 = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
        return META_PREFIX + b64;
    }

    private static List<Entry> tryDecodeMeta(Map<String, FileDataItem> data) {
        String enc = null;
        for (FileDataItem item : data.values()) {
            if (item == null) {
                continue;
            }
            String p = item.getPath();
            if (p == null) {
                continue;
            }
            int idx = p.indexOf(META_SEP);
            if (idx >= 0 && idx + 1 < p.length()) {
                String candidate = p.substring(idx + 1);
                if (candidate.startsWith(META_PREFIX)) {
                    enc = candidate;
                    break;
                }
            }
        }
        if (enc == null) {
            return null;
        }
        return decodeMeta(enc);
    }

    private static List<Entry> decodeMeta(String encoded) {
        if (encoded == null || !encoded.startsWith(META_PREFIX)) {
            return null;
        }
        String b64 = encoded.substring(META_PREFIX.length());
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(b64);
        } catch (IllegalArgumentException e) {
            return null;
        }
        String raw = new String(bytes, StandardCharsets.UTF_8);
        List<String> lines = splitLinesPreserveEmpty(normalizeNewlines(raw));
        List<Entry> out = new ArrayList<>();
        for (String line : lines) {
            if (line == null || line.isEmpty()) {
                continue;
            }
            int bar = line.indexOf('|');
            if (bar < 0) {
                continue;
            }
            String t = line.substring(0, bar);
            String payload = line.substring(bar + 1);
            if ("S".equals(t)) {
                out.add(Entry.section(unescape(payload)));
            } else if ("C".equals(t)) {
                out.add(Entry.comment(unescape(payload)));
            } else if ("E".equals(t)) {
                out.add(Entry.empty());
            } else if ("D".equals(t)) {
                out.add(Entry.data(unescape(payload)));
            }
        }
        return out;
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        String r = s.replace("\\", "\\\\");
        r = r.replace("\n", "\\n");
        r = r.replace("\r", "\\r");
        return r;
    }

    private static String unescape(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        boolean esc = false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (!esc) {
                if (ch == '\\') {
                    esc = true;
                } else {
                    sb.append(ch);
                }
            } else {
                if (ch == 'n') {
                    sb.append('\n');
                } else if (ch == 'r') {
                    sb.append('\r');
                } else if (ch == '\\') {
                    sb.append('\\');
                } else {
                    sb.append(ch);
                }
                esc = false;
            }
        }
        if (esc) {
            sb.append('\\');
        }
        return sb.toString();
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private static final class SectionKey {
        private final String section;
        private final int index;
        private final boolean hasId;
        private final String id;

        private SectionKey(String section, int index, boolean hasId, String id) {
            this.section = section;
            this.index = index;
            this.hasId = hasId;
            this.id = id;
        }
    }

    private static final class SectionItem {
        private final String section;
        private final int index;
        private final String flatKey;
        private final FileDataItem item;
        private final boolean hasId;
        private final String id;

        private SectionItem(String section, int index, String flatKey, FileDataItem item, boolean hasId, String id) {
            this.section = section;
            this.index = index;
            this.flatKey = flatKey;
            this.item = item;
            this.hasId = hasId;
            this.id = id;
        }
    }

    private static SectionKey parseSectionKey(String flatKey) {
        int lb = flatKey.lastIndexOf('[');
        int rb = flatKey.lastIndexOf(']');
        if (lb < 0 || rb < 0 || rb < lb) {
            return null;
        }
        String section = flatKey.substring(0, lb);
        String idxStr = flatKey.substring(lb + 1, rb);
        int idx;
        try {
            idx = Integer.parseInt(idxStr);
        } catch (NumberFormatException e) {
            return null;
        }
        boolean hasId = false;
        String id = null;
        if (rb + 1 < flatKey.length() && flatKey.charAt(rb + 1) == '.') {
            hasId = true;
            id = flatKey.substring(rb + 2);
        }
        if (section == null || section.isEmpty()) {
            return null;
        }
        return new SectionKey(section, idx, hasId, id);
    }
}