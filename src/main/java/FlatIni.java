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
        attachMetaToItems(result, metaB64);

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        Meta meta = extractMetaFromAnyItem(data);
        if (hasStructure(meta)) {
            return trimTrailingNewlines(dumpWithMeta(data, meta));
        }

        String fallback = looksLikeSectionedKeys(data)
                ? dumpSectionedFallback(data, DEFAULT_LS)
                : dumpPlainFallback(data, DEFAULT_LS);

        return trimTrailingNewlines(fallback);
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new UnsupportedOperationException("Validation of INI files is not implemented.");
    }

    private static boolean hasStructure(Meta meta) {
        return meta != null && meta.entries != null && !meta.entries.isEmpty();
    }

    private static void attachMetaToItems(LinkedHashMap<String, FileDataItem> result, String metaB64) {
        for (Map.Entry<String, FileDataItem> e : result.entrySet()) {
            FileDataItem item = e.getValue();
            if (item == null) {
                continue;
            }
            String base = resolveBasePath(item, e.getKey());
            if (base != null) {
                item.setPath(attachMeta(base, metaB64));
            }
        }
    }

    private static String resolveBasePath(FileDataItem item, String entryKey) {
        String base = nonBlank(item.getPath(), item.getKey());
        if (base != null) {
            return base;
        }
        return entryKey;
    }

    private static void parsePlain(List<String> lines, LinkedHashMap<String, FileDataItem> result, List<Entry> structure) {
        List<String> pendingMetaComments = new ArrayList<>();
        for (String line : lines) {
            processPlainLine(line, pendingMetaComments, result, structure);
        }
    }

    private static void processPlainLine(
            String line,
            List<String> pendingMetaComments,
            LinkedHashMap<String, FileDataItem> result,
            List<Entry> structure
    ) {
        if (line == null) {
            return;
        }

        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            addEmpty(structure, pendingMetaComments);
            return;
        }

        if (isCommentLine(trimmed)) {
            addComment(structure, trimmed, line, pendingMetaComments, null);
            return;
        }

        int eq = line.indexOf('=');
        if (eq < 0) {
            return;
        }

        String key = line.substring(0, eq).trim();
        if (key.isEmpty()) {
            return;
        }

        String value = line.substring(eq + 1).trim();
        FileDataItem item = createItem(key, key, value, joinMeta(pendingMetaComments));
        pendingMetaComments.clear();

        result.put(key, item);
        structure.add(Entry.data(key));
    }

    private static void parseSectioned(List<String> lines, LinkedHashMap<String, FileDataItem> result, List<Entry> structure) {
        SectionParseState st = new SectionParseState();
        for (String line : lines) {
            processSectionedLine(line, st, result, structure);
        }
        finalizeSectionIfEmpty(st, result, structure);
    }

    private static void processSectionedLine(
            String line,
            SectionParseState st,
            LinkedHashMap<String, FileDataItem> result,
            List<Entry> structure
    ) {
        if (line == null) {
            return;
        }

        String trimmed = line.trim();

        if (isSectionHeader(trimmed)) {
            switchSection(trimmed, st, result, structure);
            return;
        }

        if (trimmed.isEmpty()) {
            addEmpty(structure, st.pendingMetaComments);
            return;
        }

        if (isCommentLine(trimmed)) {
            addComment(structure, trimmed, line, st.pendingMetaComments, st.currentSection);
            return;
        }

        if (st.currentSection == null || st.currentSection.isEmpty()) {
            return;
        }

        commitSectionedData(trimmed, st, result, structure);
    }

    private static void switchSection(
            String trimmed,
            SectionParseState st,
            LinkedHashMap<String, FileDataItem> result,
            List<Entry> structure
    ) {
        finalizeSectionIfEmpty(st, result, structure);

        st.currentSection = trimmed.substring(1, trimmed.length() - 1).trim();
        structure.add(Entry.section(st.currentSection));

        st.indexInSection = 0;
        st.sectionHasData = false;
        st.pendingMetaComments.clear();
    }

    private static void finalizeSectionIfEmpty(
            SectionParseState st,
            LinkedHashMap<String, FileDataItem> result,
            List<Entry> structure
    ) {
        if (st.currentSection != null && !st.sectionHasData) {
            addEmptySectionPlaceholder(st.currentSection, result, structure);
        }
    }

    private static void commitSectionedData(
            String trimmed,
            SectionParseState st,
            LinkedHashMap<String, FileDataItem> result,
            List<Entry> structure
    ) {
        ParsedLine parsed = parseSectionedDataLine(trimmed, st.currentSection);
        String flatKey = buildSectionedFlatKey(st.currentSection, st.indexInSection, parsed.host);

        FileDataItem item = createItem(flatKey, flatKey, parsed.value, joinMeta(st.pendingMetaComments));
        st.pendingMetaComments.clear();

        result.put(flatKey, item);
        structure.add(Entry.data(flatKey));

        st.indexInSection++;
        st.sectionHasData = true;
    }

    private static FileDataItem createItem(String key, String path, String value, String comment) {
        FileDataItem item = new FileDataItem();
        item.setKey(key);
        item.setPath(path);
        item.setValue(value);
        if (comment != null && !comment.isEmpty()) {
            item.setComment(comment);
        }
        return item;
    }

    private static void addEmpty(List<Entry> structure, List<String> pendingMetaComments) {
        structure.add(Entry.empty());
        pendingMetaComments.clear();
    }

    private static void addComment(
            List<Entry> structure,
            String trimmed,
            String originalLine,
            List<String> pendingMetaComments,
            String section
    ) {
        structure.add(Entry.comment(originalLine));
        String content = stripCommentPrefix(trimmed);
        if (!content.isEmpty() && isMetaComment(content, section)) {
            pendingMetaComments.add(content);
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
        String ls = resolveLineSeparator(meta);
        StringBuilder sb = new StringBuilder();

        for (Entry e : meta.entries) {
            appendEntry(sb, e, data, meta, ls);
        }

        return sb.toString();
    }

    private static String resolveLineSeparator(Meta meta) {
        String ls = tokenToLineSep(meta.lineSepToken);
        if (ls == null) {
            return DEFAULT_LS;
        }
        return ls;
    }

    private static void appendEntry(
            StringBuilder sb,
            Entry e,
            Map<String, FileDataItem> data,
            Meta meta,
            String ls
    ) {
        if (e == null) {
            return;
        }

        if (e.type == EntryType.SECTION) {
            sb.append("[").append(nvl(e.payload)).append("]").append(ls);
            return;
        }

        if (e.type == EntryType.COMMENT) {
            sb.append(nvl(e.payload)).append(ls);
            return;
        }

        if (e.type == EntryType.EMPTY) {
            sb.append(ls);
            return;
        }

        if (e.type == EntryType.DATA) {
            appendDataEntry(sb, e.payload, data, meta, ls);
        }
    }

    private static void appendDataEntry(
            StringBuilder sb,
            String key,
            Map<String, FileDataItem> data,
            Meta meta,
            String ls
    ) {
        if (key == null) {
            return;
        }

        FileDataItem item = data.get(key);
        if (item == null) {
            return;
        }

        if (meta.sectioned) {
            if (!isEmptySectionPlaceholder(key, item)) {
                sb.append(formatSectionedLine(key, item)).append(ls);
            }
            return;
        }

        sb.append(formatPlainLine(key, item)).append(ls);
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

    private static String dumpPlainFallback(Map<String, FileDataItem> data, String ls) {
        List<String> keys = new ArrayList<>(data.keySet());
        Collections.sort(keys);

        StringBuilder sb = new StringBuilder();
        for (String k : keys) {
            appendPlainFallbackLine(sb, k, data, ls);
        }
        return sb.toString();
    }

    private static void appendPlainFallbackLine(StringBuilder sb, String key, Map<String, FileDataItem> data, String ls) {
        if (key == null) {
            return;
        }
        FileDataItem item = data.get(key);
        if (item == null) {
            return;
        }
        appendCommentIfAny(sb, item, ls);
        sb.append(key).append("=").append(safeValue(item)).append(ls);
    }

    private static String dumpSectionedFallback(Map<String, FileDataItem> data, String ls) {
        List<SectionedItem> items = collectSectionedItems(data);
        sortSectionedItems(items);

        StringBuilder sb = new StringBuilder();
        SectionDumpState st = new SectionDumpState();

        for (SectionedItem it : items) {
            appendSectionedFallbackLine(sb, it, data, ls, st);
        }

        return sb.toString();
    }

    private static List<SectionedItem> collectSectionedItems(Map<String, FileDataItem> data) {
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

        if (!it.section.equals(st.currentSection)) {
            if (st.currentSection != null) {
                sb.append(ls);
            }
            st.currentSection = it.section;
            sb.append("[").append(st.currentSection).append("]").append(ls);
        }

        FileDataItem item = data.get(it.originalKey);
        if (item == null) {
            return;
        }

        appendCommentIfAny(sb, item, ls);
        sb.append(formatSectionedLine(it.originalKey, item)).append(ls);
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
        if (dot < 0 || dot + 1 >= flatKey.length()) {
            return v;
        }

        String host = flatKey.substring(dot + 1);
        if (v.isEmpty()) {
            return host;
        }
        return host + " " + v;
    }

    private static int dotAfterBracket(String flatKey) {
        int close = flatKey.indexOf(']');
        if (close < 0) {
            return -1;
        }
        return flatKey.indexOf('.', close);
    }

    private static ParsedLine parseSectionedDataLine(String trimmed, String section) {
        int ws = indexOfWhitespace(trimmed);
        if (ws <= 0) {
            return new ParsedLine(null, trimmed);
        }

        String first = trimmed.substring(0, ws);
        String rest = trimmed.substring(ws).trim();

        if (rest.isEmpty()) {
            return new ParsedLine(null, trimmed);
        }

        boolean firstLooksLikeKeyValue = first.contains("=");
        if (firstLooksLikeKeyValue) {
            return new ParsedLine(null, trimmed);
        }

        if (isChildrenSection(section)) {
            return new ParsedLine(null, trimmed);
        }

        return new ParsedLine(first, rest);
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
            Meta m = decodeMetaFromItem(item);
            if (m != null) {
                return m;
            }
        }
        return null;
    }

    private static Meta decodeMetaFromItem(FileDataItem item) {
        if (item == null) {
            return null;
        }
        return decodeMetaFromPath(item.getPath());
    }

    private static Meta decodeMetaFromPath(String path) {
        String b64 = extractMetaB64(path);
        if (b64 == null) {
            return null;
        }
        return decodeMeta(b64);
    }

    private static String extractMetaB64(String path) {
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
        return b64;
    }

    private static String encodeMeta(Meta meta) {
        StringBuilder sb = new StringBuilder();
        sb.append("V2").append("\n");
        sb.append(meta.sectioned ? "1" : "0").append("\n");
        sb.append(meta.lineSepToken == null ? "LF" : meta.lineSepToken).append("\n");
        encodeEntries(sb, meta.entries);
        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void encodeEntries(StringBuilder sb, List<Entry> entries) {
        for (Entry e : entries) {
            sb.append(e.type.code).append("|");
            if (e.payload != null) {
                sb.append(Base64.getEncoder().encodeToString(e.payload.getBytes(StandardCharsets.UTF_8)));
            }
            sb.append("\n");
        }
    }

    private static Meta decodeMeta(String metaB64) {
        try {
            String text = decodeBase64ToString(metaB64);
            List<String> lines = splitLinesPreserve(text);
            MetaHeader header = parseHeader(lines);
            if (header == null) {
                return null;
            }
            List<Entry> entries = parseEntries(lines, 3);
            return new Meta(header.sectioned, header.token, entries);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static String decodeBase64ToString(String metaB64) {
        byte[] raw = Base64.getDecoder().decode(metaB64);
        return new String(raw, StandardCharsets.UTF_8);
    }

    private static MetaHeader parseHeader(List<String> lines) {
        if (lines == null || lines.size() < 3) {
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
        return new MetaHeader(sectioned, token);
    }

    private static List<Entry> parseEntries(List<String> lines, int startIndex) {
        List<Entry> entries = new ArrayList<>();
        for (int i = startIndex; i < lines.size(); i++) {
            Entry e = parseEntryLine(lines.get(i));
            if (e != null) {
                entries.add(e);
            }
        }
        return entries;
    }

    private static Entry parseEntryLine(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        int bar = line.indexOf('|');
        if (bar < 0) {
            return null;
        }

        EntryType type = EntryType.fromCode(line.charAt(0));
        if (type == null) {
            return null;
        }

        String payloadB64 = line.substring(bar + 1);
        String payload = payloadB64.isEmpty() ? null : decodePayload(payloadB64);
        return new Entry(type, payload);
    }

    private static String decodePayload(String payloadB64) {
        return new String(Base64.getDecoder().decode(payloadB64), StandardCharsets.UTF_8);
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
            if (idxStr.isEmpty()) {
                return null;
            }

            int idx = parseIndex(idxStr);
            if (idx < 0) {
                return null;
            }

            String host = parseHost(k, rb);
            return new SectionKey(section, idx, host);
        }

        private static int parseIndex(String idxStr) {
            try {
                return Integer.parseInt(idxStr);
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        private static String parseHost(String k, int rb) {
            if (rb + 1 >= k.length()) {
                return null;
            }
            if (k.charAt(rb + 1) != '.') {
                return null;
            }
            if (rb + 2 > k.length()) {
                return null;
            }
            String host = k.substring(rb + 2);
            return host.isEmpty() ? null : host;
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
        return !c.matches("^[a-z0-9._-]+\\s+.*$") || (isChildrenSection(section));
    }

    private static String joinMeta(List<String> pending) {
        if (pending == null || pending.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String s : pending) {
            appendMetaPart(sb, s);
        }
        return sb.toString();
    }

    private static void appendMetaPart(StringBuilder sb, String part) {
        if (part == null) {
            return;
        }
        String t = part.trim();
        if (t.isEmpty()) {
            return;
        }
        if (sb.length() > 0) {
            sb.append(" & ");
        }
        sb.append(t);
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

        if (!out.isEmpty() && out.get(out.size() - 1).isEmpty() && endsWithNewline(data)) {
            out.remove(out.size() - 1);
        }
        return out;
    }

    private static boolean endsWithNewline(String data) {
        return data.endsWith("\n") || data.endsWith("\r");
    }

    private static List<String> splitLinesPreserve(String data) {
        String[] raw = data.split("\\R", -1);
        List<String> out = new ArrayList<>(raw.length);
        Collections.addAll(out, raw);
        return out;
    }

    private static boolean containsSectionHeader(List<String> lines) {
        for (String line : lines) {
            if (line != null && isSectionHeader(line.trim())) {
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
        while (end > 0 && isNewlineChar(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(0, end);
    }

    private static boolean isNewlineChar(char ch) {
        return ch == '\n' || ch == '\r';
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

    private static final class SectionParseState {
        String currentSection;
        int indexInSection;
        boolean sectionHasData;
        final List<String> pendingMetaComments = new ArrayList<>();
    }

    private static final class SectionDumpState {
        String currentSection;
    }

    private static final class MetaHeader {
        final boolean sectioned;
        final String token;

        MetaHeader(boolean sectioned, String token) {
            this.sectioned = sectioned;
            this.token = token;
        }
    }
}