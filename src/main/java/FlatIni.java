import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatIni implements FlatService {

    private static final String DEFAULT_LS = System.lineSeparator();
    private static final String META_SEP = "|ini_meta|";

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        LinkedHashMap<String, FileDataItem> result = new LinkedHashMap<>();
        if (data == null || data.isBlank()) {
            return result;
        }

        String ls = detectLineSeparator(data);
        List<String> lines = splitLines(data);
        boolean sectioned = containsSectionHeader(lines);

        List<Entry> structure = new ArrayList<>();
        if (sectioned) {
            parseSectioned(lines, result, structure);
        } else {
            parsePlain(lines, result, structure);
        }

        Meta meta = new Meta(sectioned, lineSepToken(ls), structure);
        String metaB64 = encodeMeta(meta);

        for (Map.Entry<String, FileDataItem> e : result.entrySet()) {
            FileDataItem item = e.getValue();
            if (item == null) {
                continue;
            }
            String base = nonBlank(item.getPath(), item.getKey());
            if (base == null) {
                base = e.getKey();
            }
            if (base == null) {
                continue;
            }
            item.setPath(attachMeta(base, metaB64));
        }

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        Meta meta = extractMetaFromAnyItem(data);
        if (meta != null && meta.entries != null && !meta.entries.isEmpty()) {
            return trimTrailingNewlines(dumpWithMeta(data, meta));
        }

        if (looksLikeSectionedKeys(data)) {
            return trimTrailingNewlines(dumpSectionedFallback(data, DEFAULT_LS));
        }

        return trimTrailingNewlines(dumpPlainFallback(data, DEFAULT_LS));
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new UnsupportedOperationException("Validation of INI files is not implemented.");
    }

    private static void parsePlain(List<String> lines, LinkedHashMap<String, FileDataItem> result, List<Entry> structure) {
        List<String> pendingMetaComments = new ArrayList<>();

        for (String line : lines) {
            if (line == null) {
                continue;
            }

            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                structure.add(Entry.empty());
                pendingMetaComments.clear();
                continue;
            }

            if (isCommentLine(trimmed)) {
                structure.add(Entry.comment(line));
                String content = stripCommentPrefix(trimmed);
                if (!content.isEmpty() && isMetaComment(content, null)) {
                    pendingMetaComments.add(content);
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

            String comment = joinMeta(pendingMetaComments);
            if (!comment.isEmpty()) {
                item.setComment(comment);
            }
            pendingMetaComments.clear();

            result.put(key, item);
            structure.add(Entry.data(key));
        }
    }

    private static void parseSectioned(List<String> lines, LinkedHashMap<String, FileDataItem> result, List<Entry> structure) {
        String currentSection = null;
        int indexInSection = 0;
        boolean sectionHasData = false;
        List<String> pendingMetaComments = new ArrayList<>();

        for (String line : lines) {
            if (line == null) {
                continue;
            }

            String trimmed = line.trim();

            if (isSectionHeader(trimmed)) {
                if (currentSection != null && !sectionHasData) {
                    addEmptySectionPlaceholder(currentSection, result, structure);
                }

                currentSection = trimmed.substring(1, trimmed.length() - 1).trim();
                structure.add(Entry.section(currentSection));

                indexInSection = 0;
                sectionHasData = false;
                pendingMetaComments.clear();
                continue;
            }

            if (trimmed.isEmpty()) {
                structure.add(Entry.empty());
                pendingMetaComments.clear();
                continue;
            }

            if (isCommentLine(trimmed)) {
                structure.add(Entry.comment(line));
                String content = stripCommentPrefix(trimmed);
                if (!content.isEmpty() && isMetaComment(content, currentSection)) {
                    pendingMetaComments.add(content);
                }
                continue;
            }

            if (currentSection == null || currentSection.isEmpty()) {
                continue;
            }

            ParsedLine parsed = parseSectionedDataLine(trimmed, currentSection);
            String flatKey = buildSectionedFlatKey(currentSection, indexInSection, parsed.host);

            FileDataItem item = new FileDataItem();
            item.setKey(flatKey);
            item.setPath(flatKey);
            item.setValue(parsed.value);

            String comment = joinMeta(pendingMetaComments);
            if (!comment.isEmpty()) {
                item.setComment(comment);
            }
            pendingMetaComments.clear();

            result.put(flatKey, item);
            structure.add(Entry.data(flatKey));

            indexInSection++;
            sectionHasData = true;
        }

        if (currentSection != null && !sectionHasData) {
            addEmptySectionPlaceholder(currentSection, result, structure);
        }
    }

    private static void addEmptySectionPlaceholder(String section, LinkedHashMap<String, FileDataItem> result, List<Entry> structure) {
        String key = section + "[0]";
        if (result.containsKey(key)) {
            return;
        }
        FileDataItem item = new FileDataItem();
        item.setKey(key);
        item.setPath(key);
        item.setValue("");
        result.put(key, item);
        structure.add(Entry.data(key));
    }

    private static String dumpWithMeta(Map<String, FileDataItem> data, Meta meta) {
        String ls = tokenToLineSep(meta.lineSepToken);
        if (ls == null) {
            ls = DEFAULT_LS;
        }

        StringBuilder sb = new StringBuilder();

        for (Entry e : meta.entries) {
            if (e == null) {
                continue;
            }

            if (e.type == EntryType.SECTION) {
                sb.append("[").append(nvl(e.payload)).append("]").append(ls);
                continue;
            }
            if (e.type == EntryType.COMMENT) {
                sb.append(nvl(e.payload)).append(ls);
                continue;
            }
            if (e.type == EntryType.EMPTY) {
                sb.append(ls);
                continue;
            }
            if (e.type == EntryType.DATA) {
                String key = e.payload;
                if (key == null) {
                    continue;
                }
                FileDataItem item = data.get(key);
                if (item == null) {
                    continue;
                }
                if (meta.sectioned) {
                    if (!isEmptySectionPlaceholder(key, item)) {
                        sb.append(formatSectionedLine(key, item)).append(ls);
                    }
                } else {
                    sb.append(formatPlainLine(key, item)).append(ls);
                }
            }
        }

        return sb.toString();
    }

    private static boolean isEmptySectionPlaceholder(String key, FileDataItem item) {
        SectionKey sk = SectionKey.parse(key);
        if (sk == null) {
            return false;
        }
        if (sk.host != null) {
            return false;
        }
        if (sk.index != 0) {
            return false;
        }
        String v = safeValue(item);
        return v.isEmpty();
    }

    private static String dumpPlainFallback(Map<String, FileDataItem> data, String ls) {
        List<String> keys = new ArrayList<>(data.keySet());
        Collections.sort(keys);

        StringBuilder sb = new StringBuilder();
        for (String k : keys) {
            if (k == null) {
                continue;
            }
            FileDataItem item = data.get(k);
            if (item == null) {
                continue;
            }

            appendCommentIfAny(sb, item, ls);
            sb.append(k).append("=").append(safeValue(item)).append(ls);
        }
        return sb.toString();
    }

    private static String dumpSectionedFallback(Map<String, FileDataItem> data, String ls) {
        List<SectionedItem> items = new ArrayList<>();
        for (Map.Entry<String, FileDataItem> e : data.entrySet()) {
            if (e == null || e.getKey() == null) {
                continue;
            }
            SectionKey sk = SectionKey.parse(e.getKey());
            if (sk == null) {
                continue;
            }
            items.add(new SectionedItem(e.getKey(), sk.section, sk.index, sk.host));
        }

        items.sort(Comparator
                .comparing((SectionedItem it) -> it.section)
                .thenComparingInt(it -> it.index)
                .thenComparing(it -> it.host == null ? "" : it.host));

        StringBuilder sb = new StringBuilder();
        String currentSection = null;

        for (SectionedItem it : items) {
            if (!it.section.equals(currentSection)) {
                if (currentSection != null) {
                    sb.append(ls);
                }
                currentSection = it.section;
                sb.append("[").append(currentSection).append("]").append(ls);
            }

            FileDataItem item = data.get(it.originalKey);
            if (item == null) {
                continue;
            }

            appendCommentIfAny(sb, item, ls);
            sb.append(formatSectionedLine(it.originalKey, item)).append(ls);
        }

        return sb.toString();
    }

    private static void appendCommentIfAny(StringBuilder sb, FileDataItem item, String ls) {
        String c = item.getComment();
        if (c == null || c.isBlank()) {
            return;
        }
        String[] lines = c.split("\\R");
        for (String line : lines) {
            String t = line == null ? "" : line.trim();
            if (!t.isEmpty()) {
                sb.append("#").append(t).append(ls);
            }
        }
    }

    private static String formatPlainLine(String key, FileDataItem item) {
        return key + "=" + safeValue(item);
    }

    private static String formatSectionedLine(String flatKey, FileDataItem item) {
        String v = safeValue(item);

        int dot = dotAfterBracket(flatKey);
        if (dot >= 0 && dot + 1 < flatKey.length()) {
            String host = flatKey.substring(dot + 1);
            if (v.isEmpty()) {
                return host;
            }
            return host + " " + v;
        }

        return v;
    }

    private static int dotAfterBracket(String flatKey) {
        int close = flatKey.indexOf(']');
        if (close < 0) {
            return -1;
        }
        int dot = flatKey.indexOf('.', close);
        return dot;
    }

    private static ParsedLine parseSectionedDataLine(String trimmed, String section) {
        int ws = indexOfWhitespace(trimmed);
        if (ws > 0) {
            String first = trimmed.substring(0, ws);
            String rest = trimmed.substring(ws).trim();

            boolean firstLooksLikeKeyValue = first.contains("=");
            if (!firstLooksLikeKeyValue && !rest.isEmpty() && !isChildrenSection(section)) {
                return new ParsedLine(first, rest);
            }
        }
        return new ParsedLine(null, trimmed);
    }

    private static boolean isChildrenSection(String section) {
        return section != null && section.endsWith(":children");
    }

    private static int indexOfWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static String buildSectionedFlatKey(String section, int idx, String host) {
        String base = section + "[" + idx + "]";
        if (host == null || host.isEmpty()) {
            return base;
        }
        return base + "." + host;
    }

    private static String attachMeta(String basePath, String metaB64) {
        return basePath + META_SEP + metaB64;
    }

    private static Meta extractMetaFromAnyItem(Map<String, FileDataItem> data) {
        for (FileDataItem item : data.values()) {
            if (item == null) {
                continue;
            }
            Meta m = decodeMetaFromPath(item.getPath());
            if (m != null) {
                return m;
            }
        }
        return null;
    }

    private static Meta decodeMetaFromPath(String path) {
        if (path == null) {
            return null;
        }
        int idx = path.indexOf(META_SEP);
        if (idx < 0) {
            return null;
        }
        String b64 = path.substring(idx + META_SEP.length());
        if (b64.isEmpty()) {
            return null;
        }
        return decodeMeta(b64);
    }

    private static String encodeMeta(Meta meta) {
        StringBuilder sb = new StringBuilder();
        sb.append("V2").append("\n");
        sb.append(meta.sectioned ? "1" : "0").append("\n");
        sb.append(meta.lineSepToken == null ? "LF" : meta.lineSepToken).append("\n");

        for (Entry e : meta.entries) {
            sb.append(e.type.code).append("|");
            if (e.payload != null) {
                sb.append(Base64.getEncoder().encodeToString(e.payload.getBytes(StandardCharsets.UTF_8)));
            }
            sb.append("\n");
        }

        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static Meta decodeMeta(String metaB64) {
        try {
            byte[] raw = Base64.getDecoder().decode(metaB64);
            String text = new String(raw, StandardCharsets.UTF_8);

            List<String> lines = splitLinesPreserve(text);
            if (lines.size() < 3) {
                return null;
            }

            String ver = lines.get(0);
            if (!"V2".equals(ver)) {
                return null;
            }

            boolean sectioned = "1".equals(lines.get(1));
            String token = lines.get(2);
            if (token == null || token.isEmpty()) {
                token = "LF";
            }

            List<Entry> entries = new ArrayList<>();
            for (int i = 3; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line == null || line.isEmpty()) {
                    continue;
                }
                int bar = line.indexOf('|');
                if (bar < 0) {
                    continue;
                }
                char code = line.charAt(0);
                EntryType type = EntryType.fromCode(code);
                if (type == null) {
                    continue;
                }

                String payloadB64 = line.substring(bar + 1);
                String payload = payloadB64.isEmpty()
                        ? null
                        : new String(Base64.getDecoder().decode(payloadB64), StandardCharsets.UTF_8);

                entries.add(new Entry(type, payload));
            }

            return new Meta(sectioned, token, entries);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private enum EntryType {
        SECTION('S'),
        COMMENT('C'),
        EMPTY('E'),
        DATA('D');

        final char code;

        EntryType(char code) {
            this.code = code;
        }

        static EntryType fromCode(char c) {
            for (EntryType t : values()) {
                if (t.code == c) {
                    return t;
                }
            }
            return null;
        }
    }

    private static final class Entry {
        final EntryType type;
        final String payload;

        private Entry(EntryType type, String payload) {
            this.type = type;
            this.payload = payload;
        }

        static Entry section(String name) {
            return new Entry(EntryType.SECTION, name);
        }

        static Entry comment(String rawLine) {
            return new Entry(EntryType.COMMENT, rawLine);
        }

        static Entry empty() {
            return new Entry(EntryType.EMPTY, null);
        }

        static Entry data(String key) {
            return new Entry(EntryType.DATA, key);
        }
    }

    private static final class Meta {
        final boolean sectioned;
        final String lineSepToken;
        final List<Entry> entries;

        private Meta(boolean sectioned, String lineSepToken, List<Entry> entries) {
            this.sectioned = sectioned;
            this.lineSepToken = lineSepToken;
            this.entries = entries;
        }
    }

    private static final class ParsedLine {
        final String host;
        final String value;

        ParsedLine(String host, String value) {
            this.host = host;
            this.value = value;
        }
    }

    private static final class SectionedItem {
        final String originalKey;
        final String section;
        final int index;
        final String host;

        SectionedItem(String originalKey, String section, int index, String host) {
            this.originalKey = originalKey;
            this.section = section;
            this.index = index;
            this.host = host;
        }
    }

    private static final class SectionKey {
        final String section;
        final int index;
        final String host;

        private SectionKey(String section, int index, String host) {
            this.section = section;
            this.index = index;
            this.host = host;
        }

        static SectionKey parse(String k) {
            if (k == null) {
                return null;
            }
            int lb = k.indexOf('[');
            int rb = k.indexOf(']');
            if (lb <= 0 || rb < lb) {
                return null;
            }
            String section = k.substring(0, lb);
            String idxStr = k.substring(lb + 1, rb);
            if (section.isEmpty() || idxStr.isEmpty()) {
                return null;
            }
            int idx;
            try {
                idx = Integer.parseInt(idxStr);
            } catch (NumberFormatException e) {
                return null;
            }
            String host = null;
            if (rb + 1 < k.length() && k.charAt(rb + 1) == '.' && rb + 2 <= k.length()) {
                host = k.substring(rb + 2);
                if (host.isEmpty()) {
                    host = null;
                }
            }
            return new SectionKey(section, idx, host);
        }
    }

    private static boolean isCommentLine(String trimmed) {
        return trimmed.startsWith("#") || trimmed.startsWith(";");
    }

    private static String stripCommentPrefix(String trimmed) {
        String s = trimmed;
        if (s.startsWith("#") || s.startsWith(";")) {
            s = s.substring(1);
        }
        return s.trim();
    }

    private static boolean isMetaComment(String content, String section) {
        String c = content.toLowerCase();
        if (c.contains("ansible_")) {
            return false;
        }
        if (c.matches("^[a-z0-9._-]+\\s+.*$") && (section == null || !isChildrenSection(section))) {
            return false;
        }
        return true;
    }

    private static String joinMeta(List<String> pending) {
        if (pending == null || pending.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String s : pending) {
            if (s == null) {
                continue;
            }
            String t = s.trim();
            if (t.isEmpty()) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(" & ");
            }
            sb.append(t);
        }
        return sb.toString();
    }

    private static boolean looksLikeSectionedKeys(Map<String, FileDataItem> data) {
        for (String k : data.keySet()) {
            if (k != null && k.contains("[") && k.contains("]")) {
                return true;
            }
        }
        return false;
    }

    private static List<String> splitLines(String data) {
        String[] raw = data.split("\\R", -1);
        List<String> out = new ArrayList<>(raw.length);
        Collections.addAll(out, raw);

        if (!out.isEmpty() && out.get(out.size() - 1).isEmpty()
                && (data.endsWith("\n") || data.endsWith("\r"))) {
            out.remove(out.size() - 1);
        }
        return out;
    }

    private static List<String> splitLinesPreserve(String data) {
        String[] raw = data.split("\\R", -1);
        List<String> out = new ArrayList<>(raw.length);
        Collections.addAll(out, raw);
        return out;
    }

    private static boolean containsSectionHeader(List<String> lines) {
        for (String line : lines) {
            if (line == null) {
                continue;
            }
            if (isSectionHeader(line.trim())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.length() >= 3 && trimmed.startsWith("[") && trimmed.endsWith("]");
    }

    private static String detectLineSeparator(String data) {
        int rn = data.indexOf("\r\n");
        if (rn >= 0) {
            return "\r\n";
        }
        int r = data.indexOf('\r');
        if (r >= 0) {
            return "\r";
        }
        return "\n";
    }

    private static String safeValue(FileDataItem item) {
        if (item == null) {
            return "";
        }
        Object v = item.getValue();
        return v == null ? "" : String.valueOf(v);
    }

    private static String trimTrailingNewlines(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        int end = s.length();
        while (end > 0) {
            char ch = s.charAt(end - 1);
            if (ch == '\n' || ch == '\r') {
                end--;
                continue;
            }
            break;
        }
        return s.substring(0, end);
    }

    private static String nonBlank(String a, String b) {
        if (a != null && !a.isBlank()) {
            return a;
        }
        if (b != null && !b.isBlank()) {
            return b;
        }
        return null;
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    private static String lineSepToken(String ls) {
        if ("\r\n".equals(ls)) {
            return "CRLF";
        }
        if ("\r".equals(ls)) {
            return "CR";
        }
        return "LF";
    }

    private static String tokenToLineSep(String token) {
        if (token == null) {
            return DEFAULT_LS;
        }
        if ("CRLF".equals(token)) {
            return "\r\n";
        }
        if ("CR".equals(token)) {
            return "\r";
        }
        if ("LF".equals(token)) {
            return "\n";
        }
        return DEFAULT_LS;
    }
}