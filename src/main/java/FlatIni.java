import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FlatIni implements FlatService {

    private static final String DEFAULT_LS = System.lineSeparator();
    private static final String META_KEY = "\u0000__flat_ini_meta__";
    private static final String META_VERSION = "V1";
    private static final String FIELD_SEP = "\u0001";
    private static final String LEGACY_META_SEP = "|ini_meta|";
    private static final Pattern HOST_LINE_PATTERN = Pattern.compile("^[a-z0-9._-]+\\s+.*$");

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        MetaHidingMap result = new MetaHidingMap();
        if (data == null || data.isBlank()) {
            return result;
        }

        ParseInput input = parseInput(data);
        boolean sectioned = looksLikeSectionedIni(input.lines);
        ParseState st = new ParseState(sectioned);

        for (String line : input.lines) {
            if (line == null) {
                continue;
            }
            parseLine(line, st, result);
        }

        st.finish(result);

        MetaState meta = new MetaState(sectioned, input.lineSepToken, input.originalEndsWithNewline, st.structure);
        FileDataItem metaItem = new FileDataItem();
        metaItem.setKey(META_KEY);
        metaItem.setPath(META_KEY);
        metaItem.setValue(encodeMeta(meta));
        result.putInternal(META_KEY, metaItem);

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        MetaState meta = readMetaState(data);
        if (meta == null || meta.structure == null || meta.structure.isEmpty()) {
            String out = dumpFallback(data, DEFAULT_LS);
            return trimTrailingIfNeeded(out, DEFAULT_LS);
        }

        String ls = tokenToLineSep(meta.lineSepToken);
        String out = dumpWithStructure(data, meta, ls);

        if (!meta.originalEndsWithNewline) {
            out = trimTrailingIfNeeded(out, ls);
        }
        return out;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new UnsupportedOperationException("Validation of ini/inventory files is not implemented.");
    }

    private static ParseInput parseInput(String data) {
        String ls = detectLineSeparator(data);
        boolean endsWithNewline = endsWithNewline(data);

        List<String> lines = splitLines(data);

        if (endsWithNewline && !lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
            lines.remove(lines.size() - 1);
        }

        return new ParseInput(lines, lineSepToken(ls), endsWithNewline);
    }

    private static List<String> splitLines(String data) {
        String[] raw = data.split("\\R", -1);
        List<String> lines = new ArrayList<>(raw.length);
        Collections.addAll(lines, raw);
        return lines;
    }

    private static boolean looksLikeSectionedIni(List<String> lines) {
        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (isCommentLine(trimmed)) {
                continue;
            }
            if (isSectionHeader(trimmed)) {
                return true;
            }
            return false;
        }
        return false;
    }

    private static void parseLine(String rawLine, ParseState st, Map<String, FileDataItem> result) {
        String trimmed = rawLine.trim();

        if (trimmed.isEmpty()) {
            st.addEmpty();
            return;
        }

        if (isCommentLine(trimmed)) {
            st.addComment(rawLine, trimmed);
            return;
        }

        if (isSectionHeader(trimmed)) {
            st.switchSection(rawLine, trimmed, result);
            return;
        }

        if (st.sectioned) {
            st.addSectionedData(rawLine, trimmed, result);
            return;
        }

        st.addPlainData(rawLine, result);
    }

    private static String dumpWithStructure(Map<String, FileDataItem> data, MetaState meta, String ls) {
        StringBuilder sb = new StringBuilder();
        for (LineEntry e : meta.structure) {
            appendEntry(sb, e, data, meta.sectioned, ls);
        }
        return sb.toString();
    }

    private static void appendEntry(
            StringBuilder sb,
            LineEntry e,
            Map<String, FileDataItem> data,
            boolean sectioned,
            String ls
    ) {
        if (e == null) {
            return;
        }

        switch (e.type) {
            case SECTION:
            case COMMENT:
                sb.append(nvl(e.text)).append(ls);
                break;
            case EMPTY:
                sb.append(ls);
                break;
            case DATA:
                appendData(sb, e, data, sectioned, ls);
                break;
            default:
                break;
        }
    }

    private static void appendData(
            StringBuilder sb,
            LineEntry e,
            Map<String, FileDataItem> data,
            boolean sectioned,
            String ls
    ) {
        if (e.key == null) {
            return;
        }
        FileDataItem item = data.get(e.key);
        if (item == null) {
            return;
        }

        if (sectioned && isEmptySectionPlaceholder(e.key, item)) {
            return;
        }

        String value = safeValue(item);

        String prefix = nvl(e.prefix);
        String suffix = nvl(e.suffix);

        if (prefix.isEmpty()) {
            sb.append(value);
        } else {
            sb.append(prefix).append(value);
        }
        if (!suffix.isEmpty()) {
            sb.append(suffix);
        }
        sb.append(ls);
    }

    private static String dumpFallback(Map<String, FileDataItem> data, String ls) {
        if (looksLikeSectionedKeys(data.keySet())) {
            return dumpSectionedFallback(data, ls);
        }
        return dumpPlainFallback(data, ls);
    }

    private static boolean looksLikeSectionedKeys(Set<String> keys) {
        for (String k : keys) {
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            if (SectionKey.parse(k) != null) {
                return true;
            }
        }
        return false;
    }

    private static String dumpPlainFallback(Map<String, FileDataItem> data, String ls) {
        List<String> keys = new ArrayList<>();
        for (String k : data.keySet()) {
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            keys.add(k);
        }
        Collections.sort(keys);

        StringBuilder sb = new StringBuilder();
        for (String k : keys) {
            FileDataItem item = data.get(k);
            if (item == null) {
                continue;
            }
            appendCommentIfAny(sb, item.getComment(), ls);
            sb.append(k).append("=").append(safeValue(item)).append(ls);
        }
        return trimTrailingIfNeeded(sb.toString(), ls);
    }

    private static void appendCommentIfAny(StringBuilder sb, String comment, String ls) {
        if (comment == null || comment.isBlank()) {
            return;
        }
        String[] parts = comment.split("\\R", -1);
        for (String p : parts) {
            if (p == null) {
                continue;
            }
            String t = p.trim();
            if (t.isEmpty()) {
                continue;
            }
            sb.append("#").append(t).append(ls);
        }
    }

    private static String dumpSectionedFallback(Map<String, FileDataItem> data, String ls) {
        List<SectionedItem> items = collectSectionedItems(data);
        sortSectionedItems(items);

        StringBuilder sb = new StringBuilder();
        SectionDumpState st = new SectionDumpState();

        for (SectionedItem it : items) {
            appendSectionedFallbackLine(sb, it, data, ls, st);
        }

        return trimTrailingIfNeeded(sb.toString(), ls);
    }

    private static List<SectionedItem> collectSectionedItems(Map<String, FileDataItem> data) {
        List<SectionedItem> items = new ArrayList<>();
        for (Map.Entry<String, FileDataItem> e : data.entrySet()) {
            String k = e.getKey();
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            SectionKey sk = SectionKey.parse(k);
            if (sk == null) {
                continue;
            }
            items.add(new SectionedItem(k, sk.section, sk.index, sk.host));
        }
        return items;
    }

    private static void sortSectionedItems(List<SectionedItem> items) {
        items.sort(Comparator
                .comparing((SectionedItem it) -> it.section)
                .thenComparingInt(it -> it.index)
                .thenComparing(it -> it.host == null ? "" : it.host));
    }

    private static void appendSectionedFallbackLine(
            StringBuilder sb,
            SectionedItem it,
            Map<String, FileDataItem> data,
            String ls,
            SectionDumpState st
    ) {
        if (it == null) {
            return;
        }

        if (st.currentSection == null || !it.section.equals(st.currentSection)) {
            st.currentSection = it.section;
            sb.append("[").append(it.section).append("]").append(ls);
        }

        FileDataItem item = data.get(it.originalKey);
        if (item == null) {
            return;
        }

        appendCommentIfAny(sb, item.getComment(), ls);

        String v = safeValue(item);
        if (it.host != null && !it.host.isEmpty()) {
            if (v.isEmpty()) {
                sb.append(it.host).append(ls);
            } else {
                sb.append(it.host).append(" ").append(v).append(ls);
            }
        } else {
            if (!v.isEmpty()) {
                sb.append(v).append(ls);
            }
        }
    }

    private static MetaState readMetaState(Map<String, FileDataItem> data) {
        FileDataItem metaItem = data.get(META_KEY);
        if (metaItem == null) {
            return readLegacyMetaFromItemPaths(data);
        }
        Object v = metaItem.getValue();
        if (v == null) {
            return null;
        }
        String b64 = String.valueOf(v);
        if (b64.isEmpty()) {
            return null;
        }
        return decodeMeta(b64);
    }

    private static MetaState readLegacyMetaFromItemPaths(Map<String, FileDataItem> data) {
        for (Map.Entry<String, FileDataItem> e : data.entrySet()) {
            FileDataItem item = e.getValue();
            if (item == null) {
                continue;
            }
            MetaState m = decodeLegacyMetaFromPath(item.getPath());
            if (m != null) {
                return m;
            }
        }
        return null;
    }

    private static MetaState decodeLegacyMetaFromPath(String path) {
        if (path == null) {
            return null;
        }
        int i = path.indexOf(LEGACY_META_SEP);
        if (i < 0) {
            return null;
        }
        String metaB64 = path.substring(i + LEGACY_META_SEP.length());
        if (metaB64.isEmpty()) {
            return null;
        }
        return decodeMeta(metaB64);
    }

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.length() >= 3 && trimmed.startsWith("[") && trimmed.endsWith("]");
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
        String c = content.toLowerCase(Locale.ROOT);
        if (c.contains("ansible_")) {
            return false;
        }
        if (isChildrenSection(section)) {
            return true;
        }
        return !HOST_LINE_PATTERN.matcher(c).matches();
    }

    private static boolean isChildrenSection(String section) {
        return section != null && section.endsWith(":children");
    }

    private static String safeValue(FileDataItem item) {
        if (item == null) {
            return "";
        }
        Object v = item.getValue();
        if (v == null) {
            return "";
        }
        return String.valueOf(v);
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
        return safeValue(item).isEmpty();
    }

    private static boolean isHostKey(String key) {
        SectionKey sk = SectionKey.parse(key);
        return sk != null && sk.host != null;
    }

    private static String buildSectionedFlatKey(String section, int idx, String host) {
        String base = section + "[" + idx + "]";
        if (host == null || host.isEmpty()) {
            return base;
        }
        return base + "." + host;
    }

    private static String trimTrailingIfNeeded(String text, String ls) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        if (ls != null && !ls.isEmpty() && text.endsWith(ls)) {
            return text.substring(0, text.length() - ls.length());
        }
        int end = text.length();
        while (end > 0 && isNewlineChar(text.charAt(end - 1))) {
            end--;
        }
        return text.substring(0, end);
    }

    private static boolean isNewlineChar(char ch) {
        return ch == '\n' || ch == '\r';
    }

    private static String detectLineSeparator(String data) {
        if (data.indexOf("\r\n") >= 0) {
            return "\r\n";
        }
        if (data.indexOf('\n') >= 0) {
            return "\n";
        }
        if (data.indexOf('\r') >= 0) {
            return "\r";
        }
        return DEFAULT_LS;
    }

    private static boolean endsWithNewline(String data) {
        return data.endsWith("\n") || data.endsWith("\r");
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

    private static String encodeMeta(MetaState meta) {
        StringBuilder sb = new StringBuilder();
        sb.append(META_VERSION).append("\n");
        sb.append(meta.sectioned ? "1" : "0").append("\n");
        sb.append(meta.originalEndsWithNewline ? "1" : "0").append("\n");
        sb.append(meta.lineSepToken == null ? "LF" : meta.lineSepToken).append("\n");

        for (LineEntry e : meta.structure) {
            if (e == null) {
                continue;
            }
            sb.append(e.type.code).append("|");
            if (e.type == LineType.DATA) {
                String payload = nvl(e.key) + FIELD_SEP + nvl(e.prefix) + FIELD_SEP + nvl(e.suffix);
                sb.append(encodePayload(payload));
            } else if (e.type == LineType.SECTION || e.type == LineType.COMMENT) {
                sb.append(encodePayload(nvl(e.text)));
            }
            sb.append("\n");
        }

        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String encodePayload(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static MetaState decodeMeta(String metaB64) {
        try {
            String text = new String(Base64.getDecoder().decode(metaB64), StandardCharsets.UTF_8);
            List<String> lines = splitLinesPreserve(text);
            if (lines.size() < 4) {
                return null;
            }
            if (!META_VERSION.equals(lines.get(0))) {
                return null;
            }
            boolean sectioned = "1".equals(lines.get(1));
            boolean endsWithNewline = "1".equals(lines.get(2));
            String token = lines.get(3);
            if (token == null || token.isEmpty()) {
                token = "LF";
            }

            List<LineEntry> structure = new ArrayList<>();
            for (int i = 4; i < lines.size(); i++) {
                LineEntry e = parseEntryLine(lines.get(i));
                if (e != null) {
                    structure.add(e);
                }
            }
            return new MetaState(sectioned, token, endsWithNewline, structure);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static List<String> splitLinesPreserve(String data) {
        String[] raw = data.split("\\R", -1);
        List<String> out = new ArrayList<>(raw.length);
        Collections.addAll(out, raw);
        return out;
    }

    private static LineEntry parseEntryLine(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        int bar = line.indexOf('|');
        if (bar < 0) {
            return null;
        }

        LineType type = LineType.fromCode(line.charAt(0));
        if (type == null) {
            return null;
        }

        String payloadB64 = line.substring(bar + 1);
        String payload = payloadB64.isEmpty() ? "" : decodePayload(payloadB64);

        switch (type) {
            case EMPTY:
                return LineEntry.empty();
            case SECTION:
                return LineEntry.section(payload);
            case COMMENT:
                return LineEntry.comment(payload);
            case DATA:
                return parseDataEntryPayload(payload);
            default:
                return null;
        }
    }

    private static LineEntry parseDataEntryPayload(String payload) {
        String p = payload == null ? "" : payload;
        String[] parts = p.split(FIELD_SEP, -1);
        String key = parts.length > 0 ? parts[0] : "";
        String prefix = parts.length > 1 ? parts[1] : "";
        String suffix = parts.length > 2 ? parts[2] : "";
        if (key.isEmpty()) {
            return null;
        }
        return LineEntry.data(key, prefix, suffix);
    }

    private static String decodePayload(String payloadB64) {
        return new String(Base64.getDecoder().decode(payloadB64), StandardCharsets.UTF_8);
    }

    private static String rtrim(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        int end = s.length();
        while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(0, end);
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    private static FileDataItem createItem(String key, String value, String comment) {
        FileDataItem item = new FileDataItem();
        item.setKey(key);
        item.setPath(key);
        item.setValue(value == null ? "" : value);
        if (comment != null && !comment.isBlank()) {
            item.setComment(comment);
        }
        return item;
    }

    private enum LineType {
        SECTION('S'),
        COMMENT('C'),
        EMPTY('E'),
        DATA('D');

        final char code;

        LineType(char code) {
            this.code = code;
        }

        static LineType fromCode(char c) {
            for (LineType t : values()) {
                if (t.code == c) {
                    return t;
                }
            }
            return null;
        }
    }

    private static final class LineEntry {
        final LineType type;
        final String text;
        final String key;
        final String prefix;
        final String suffix;

        private LineEntry(LineType type, String text, String key, String prefix, String suffix) {
            this.type = type;
            this.text = text;
            this.key = key;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        static LineEntry section(String rawLine) {
            return new LineEntry(LineType.SECTION, rawLine, null, null, null);
        }

        static LineEntry comment(String rawLine) {
            return new LineEntry(LineType.COMMENT, rawLine, null, null, null);
        }

        static LineEntry empty() {
            return new LineEntry(LineType.EMPTY, null, null, null, null);
        }

        static LineEntry data(String key, String prefix, String suffix) {
            return new LineEntry(LineType.DATA, null, key, prefix, suffix);
        }
    }

    private static final class MetaState {
        final boolean sectioned;
        final String lineSepToken;
        final boolean originalEndsWithNewline;
        final List<LineEntry> structure;

        MetaState(boolean sectioned, String lineSepToken, boolean originalEndsWithNewline, List<LineEntry> structure) {
            this.sectioned = sectioned;
            this.lineSepToken = lineSepToken;
            this.originalEndsWithNewline = originalEndsWithNewline;
            this.structure = structure;
        }
    }

    private static final class ParseInput {
        final List<String> lines;
        final String lineSepToken;
        final boolean originalEndsWithNewline;

        ParseInput(List<String> lines, String lineSepToken, boolean originalEndsWithNewline) {
            this.lines = lines;
            this.lineSepToken = lineSepToken;
            this.originalEndsWithNewline = originalEndsWithNewline;
        }
    }

    private static final class ParseState {
        final boolean sectioned;
        String currentSection;
        int indexInSection;
        boolean sectionHasData;
        final List<String> pendingMetaComments = new ArrayList<>();
        final List<LineEntry> structure = new ArrayList<>();

        ParseState(boolean sectioned) {
            this.sectioned = sectioned;
        }

        void switchSection(String rawLine, String trimmedLine, Map<String, FileDataItem> result) {
            finishSectionIfEmpty(result);
            String name = extractSectionName(trimmedLine);
            currentSection = name;
            indexInSection = 0;
            sectionHasData = false;
            pendingMetaComments.clear();
            structure.add(LineEntry.section(rawLine));
        }

        void addComment(String rawLine, String trimmedLine) {
            structure.add(LineEntry.comment(rawLine));
            String content = stripCommentPrefix(trimmedLine);
            if (content.isEmpty()) {
                return;
            }
            if (isMetaComment(content, currentSection)) {
                pendingMetaComments.add(content);
            }
        }

        void addEmpty() {
            structure.add(LineEntry.empty());
            pendingMetaComments.clear();
        }

        void addPlainData(String rawLine, Map<String, FileDataItem> result) {
            int eq = rawLine.indexOf('=');
            if (eq < 0) {
                return;
            }
            String left = rawLine.substring(0, eq);
            String key = left.trim();
            if (key.isEmpty()) {
                return;
            }

            String after = rawLine.substring(eq + 1);
            int l = firstNonWhitespace(after);
            int r = lastNonWhitespaceExclusive(after);

            String prefix = rawLine.substring(0, eq + 1) + after.substring(0, l);
            String value = after.substring(l, r);
            String suffix = after.substring(r);

            FileDataItem item = createItem(key, value, joinMeta());
            result.put(key, item);
            structure.add(LineEntry.data(key, prefix, suffix));
        }

        void addSectionedData(String rawLine, String trimmed, Map<String, FileDataItem> result) {
            if (currentSection == null) {
                return;
            }

            ParsedSectionedLine pl = parseSectionedLine(trimmed, currentSection);
            String key = buildSectionedFlatKey(currentSection, indexInSection, pl.host);
            indexInSection++;

            FileDataItem item = createItem(key, pl.value, joinMeta());
            result.put(key, item);

            structure.add(LineEntry.data(key, pl.prefix, pl.suffix));

            if (!isEmptySectionPlaceholder(key, item)) {
                sectionHasData = true;
            }
        }

        void finish(Map<String, FileDataItem> result) {
            finishSectionIfEmpty(result);
        }

        private void finishSectionIfEmpty(Map<String, FileDataItem> result) {
            if (!sectioned) {
                return;
            }
            if (currentSection == null) {
                return;
            }
            if (sectionHasData) {
                return;
            }
            if (result == null) {
                return;
            }
            String key = buildSectionedFlatKey(currentSection, 0, null);
            if (!result.containsKey(key)) {
                FileDataItem item = createItem(key, "", "");
                result.put(key, item);
            }
            structure.add(LineEntry.data(key, "", ""));
        }

        private String extractSectionName(String trimmed) {
            if (!isSectionHeader(trimmed)) {
                return null;
            }
            String inside = trimmed.substring(1, trimmed.length() - 1).trim();
            return inside.isEmpty() ? null : inside;
        }

        private String joinMeta() {
            if (pendingMetaComments.isEmpty()) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            for (String s : pendingMetaComments) {
                String t = s == null ? "" : s.trim();
                if (t.isEmpty()) {
                    continue;
                }
                if (sb.length() > 0) {
                    sb.append(" & ");
                }
                sb.append(t);
            }
            pendingMetaComments.clear();
            return sb.toString();
        }
    }

    private static final class ParsedSectionedLine {
        final String host;
        final String value;
        final String prefix;
        final String suffix;

        ParsedSectionedLine(String host, String value, String prefix, String suffix) {
            this.host = host;
            this.value = value;
            this.prefix = prefix;
            this.suffix = suffix;
        }
    }

    private static ParsedSectionedLine parseSectionedLine(String trimmed, String section) {
        int ws = indexOfWhitespace(trimmed);
        if (ws <= 0) {
            return new ParsedSectionedLine(null, trimmed, "", "");
        }

        String first = trimmed.substring(0, ws);
        String restWithWs = trimmed.substring(ws);
        int restStart = firstNonWhitespace(restWithWs);
        String sep = restWithWs.substring(0, restStart);
        String rest = restWithWs.substring(restStart).trim();

        if (rest.isEmpty()) {
            return new ParsedSectionedLine(null, trimmed, "", "");
        }

        if (first.contains("=")) {
            return new ParsedSectionedLine(null, trimmed, "", "");
        }

        if (isChildrenSection(section)) {
            return new ParsedSectionedLine(null, trimmed, "", "");
        }

        String prefix = first + sep;
        return new ParsedSectionedLine(first, rest, prefix, "");
    }

    private static int indexOfWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static int firstNonWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
        return s.length();
    }

    private static int lastNonWhitespaceExclusive(String s) {
        int end = s.length();
        while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return end;
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
            if (META_KEY.equals(k)) {
                return null;
            }
            int lb = k.indexOf('[');
            int rb = k.indexOf(']');
            if (lb <= 0 || rb < lb) {
                return null;
            }

            String section = k.substring(0, lb);
            String idxStr = k.substring(lb + 1, rb);
            if (idxStr.isEmpty()) {
                return null;
            }

            int idx;
            try {
                idx = Integer.parseInt(idxStr);
            } catch (NumberFormatException e) {
                return null;
            }

            String host = parseHost(k, rb);
            return new SectionKey(section, idx, host);
        }

        private static String parseHost(String k, int rb) {
            if (rb + 1 >= k.length()) {
                return null;
            }
            if (k.charAt(rb + 1) != '.') {
                return null;
            }
            String host = k.substring(rb + 2);
            return host.isEmpty() ? null : host;
        }
    }

    private static final class SectionDumpState {
        String currentSection;
    }

    private static final class MetaHidingMap extends LinkedHashMap<String, FileDataItem> {
        void putInternal(String key, FileDataItem value) {
            super.put(key, value);
        }

        @Override
        public Set<String> keySet() {
            Set<String> base = super.keySet();
            if (!base.contains(META_KEY)) {
                return base;
            }
            Set<String> out = new LinkedHashSet<>();
            for (String k : base) {
                if (!META_KEY.equals(k)) {
                    out.add(k);
                }
            }
            return out;
        }

        @Override
        public Collection<FileDataItem> values() {
            if (!super.containsKey(META_KEY)) {
                return super.values();
            }
            List<FileDataItem> out = new ArrayList<>();
            for (Map.Entry<String, FileDataItem> e : super.entrySet()) {
                if (!META_KEY.equals(e.getKey())) {
                    out.add(e.getValue());
                }
            }
            return out;
        }

        @Override
        public int size() {
            int s = super.size();
            return super.containsKey(META_KEY) ? s - 1 : s;
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean containsKey(Object key) {
            if (META_KEY.equals(key)) {
                return false;
            }
            return super.containsKey(key);
        }
    }
}