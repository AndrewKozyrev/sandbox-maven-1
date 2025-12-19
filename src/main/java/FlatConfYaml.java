import org.apache.commons.lang3.NotImplementedException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FlatConfYaml implements FlatService {

    private static final String DEFAULT_LS = "\n";
    private static final String META_KEY = "\u0000__flat_conf_yml_meta__";
    private static final String META_VERSION = "V1";
    private static final String FIELD_SEP = "\u0001";

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        MetaHidingMap result = new MetaHidingMap();
        if (data == null || data.isBlank()) {
            return result;
        }

        ParseInput input = parseInputText(data);
        State state = new State(input.endsWithNewline, input.lineSeparator, DEFAULT_LS);

        parseLines(input.lines, result, state);

        FileDataItem metaItem = new FileDataItem();
        metaItem.setKey(META_KEY);
        metaItem.setPath(META_KEY);
        metaItem.setValue(encodeMeta(state));
        result.putInternal(META_KEY, metaItem);

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        State state = readMetaState(data);
        if (state == null || state.structure.isEmpty()) {
            return dumpFallback(data, DEFAULT_LS);
        }

        String out = dumpWithStructure(data, state, DEFAULT_LS);
        if (!state.originalEndsWithNewline) {
            out = trimTrailingIfNeeded(out, state.lineSeparator);
        }
        return out;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation of .conf.yml files is not implemented yet.");
    }

    private static ParseInput parseInputText(String data) {
        String lineSeparator = detectLineSeparator(data);
        boolean endsWithNewline = data.endsWith("\n") || data.endsWith("\r");

        String[] raw = data.split("\\R", -1);
        List<String> lines = new ArrayList<>();
        Collections.addAll(lines, raw);

        if (endsWithNewline && !lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
            lines.remove(lines.size() - 1);
        }

        return new ParseInput(lines, lineSeparator, endsWithNewline);
    }

    private static void parseLines(List<String> lines, Map<String, FileDataItem> result, State state) {
        ParseState ps = new ParseState();

        for (String line : lines) {
            if (line == null) {
                continue;
            }

            boolean handled = tryHandleEmptyLine(line, ps, state);
            if (!handled) {
                handled = tryHandleCommentLine(line, ps, state);
            }
            if (!handled) {
                handled = tryHandleContinuation(line, ps);
            }
            if (!handled) {
                handled = tryHandleTopLevelKey(line, ps, result, state);
            }

            if (!handled) {
                state.structure.add(LineEntry.raw(line));
                ps.pendingMeta.setLength(0);
            }
        }

        commitCurrent(result, ps);
    }

    private static boolean tryHandleEmptyLine(String line, ParseState ps, State state) {
        if (!line.trim().isEmpty()) {
            return false;
        }
        state.structure.add(LineEntry.empty());
        ps.pendingMeta.setLength(0);
        return true;
    }

    private static boolean tryHandleCommentLine(String line, ParseState ps, State state) {
        String trimmed = line.trim();
        if (!trimmed.startsWith("#")) {
            return false;
        }
        state.structure.add(LineEntry.comment(line));
        appendPendingMeta(ps.pendingMeta, trimmed.substring(1).trim());
        return true;
    }

    private static void appendPendingMeta(StringBuilder pendingMeta, String metaLine) {
        if (metaLine == null) {
            return;
        }
        if (pendingMeta.length() > 0) {
            pendingMeta.append('\n');
        }
        pendingMeta.append(metaLine);
    }

    private static boolean tryHandleContinuation(String line, ParseState ps) {
        if (ps.currentKey == null) {
            return false;
        }
        String trimmed = line.trim();
        boolean indented = startsWithIndent(line);
        boolean listItem = trimmed.startsWith("- ");
        if (!indented && !listItem) {
            return false;
        }
        ps.currentValue.append('\n').append(line);
        return true;
    }

    private static boolean startsWithIndent(String line) {
        if (line.isEmpty()) {
            return false;
        }
        char c0 = line.charAt(0);
        return c0 == ' ' || c0 == '\t';
    }

    private static boolean tryHandleTopLevelKey(String line, ParseState ps, Map<String, FileDataItem> result, State state) {
        if (!isTopLevelKeyLine(line)) {
            return false;
        }

        commitCurrent(result, ps);

        TopKeyParts parts = splitTopLevelKey(line);
        if (parts == null) {
            state.structure.add(LineEntry.raw(line));
            ps.pendingMeta.setLength(0);
            return true;
        }

        ps.currentKey = parts.key;
        ps.currentPrefix = parts.prefix;
        ps.currentSuffix = parts.suffix;
        ps.currentValue = new StringBuilder(parts.value);

        state.structure.add(LineEntry.forKey(ps.currentKey, ps.currentPrefix, ps.currentSuffix));
        return true;
    }

    private static TopKeyParts splitTopLevelKey(String line) {
        int colonIdx = line.indexOf(':');
        if (colonIdx <= 0) {
            return null;
        }

        String keyRaw = line.substring(0, colonIdx);
        String key = keyRaw.trim();
        if (key.isEmpty()) {
            return null;
        }

        String afterColon = line.substring(colonIdx + 1);
        int commentIdx = findInlineCommentIndex(afterColon);

        String nonCommentPart = commentIdx >= 0 ? afterColon.substring(0, commentIdx) : afterColon;
        String commentPart = commentIdx >= 0 ? afterColon.substring(commentIdx) : "";

        Range trimmedRange = trimRange(nonCommentPart);
        String valueText = "";
        String prefixSpaces = "";
        String suffixSpaces = "";
        if (trimmedRange != null) {
            int valueStart = trimmedRange.start;
            int valueEnd = trimmedRange.end;
            prefixSpaces = nonCommentPart.substring(0, valueStart);
            suffixSpaces = nonCommentPart.substring(valueEnd);
            valueText = nonCommentPart.substring(valueStart, valueEnd).trim();
        }

        String prefix = line.substring(0, colonIdx + 1) + prefixSpaces;
        String suffix = suffixSpaces + commentPart;

        return new TopKeyParts(key, prefix, suffix, valueText);
    }

    private static Range trimRange(String s) {
        if (s == null) {
            return null;
        }
        int start = 0;
        while (start < s.length() && Character.isWhitespace(s.charAt(start))) {
            start++;
        }
        int end = s.length();
        while (end > start && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return new Range(start, end);
    }

    private static void commitCurrent(Map<String, FileDataItem> result, ParseState ps) {
        if (ps.currentKey == null) {
            return;
        }

        FileDataItem item = new FileDataItem();
        item.setKey(ps.currentKey);
        item.setPath(ps.currentKey);
        item.setValue(ps.currentValue == null ? "" : ps.currentValue.toString());

        if (ps.pendingMeta.length() > 0) {
            item.setComment(ps.pendingMeta.toString());
        }

        result.put(ps.currentKey, item);

        ps.currentKey = null;
        ps.currentPrefix = null;
        ps.currentSuffix = null;
        ps.currentValue = new StringBuilder();
        ps.pendingMeta.setLength(0);
    }

    private static boolean isTopLevelKeyLine(String line) {
        if (line == null || line.isEmpty()) {
            return false;
        }
        char c0 = line.charAt(0);
        if (c0 == ' ' || c0 == '\t') {
            return false;
        }
        if (line.startsWith("- ")) {
            return false;
        }
        int idx = line.indexOf(':');
        return idx > 0;
    }

    private static int findInlineCommentIndex(String s) {
        if (s == null || s.isEmpty()) {
            return -1;
        }

        boolean inSingle = false;
        boolean inDouble = false;

        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == '\'' && !inDouble) {
                inSingle = !inSingle;
            } else if (ch == '"' && !inSingle) {
                inDouble = !inDouble;
            } else if (ch == '#' && !inSingle && !inDouble) {
                return i;
            }
        }
        return -1;
    }

    private static String dumpWithStructure(Map<String, FileDataItem> data, State state, String defaultLs) {
        String ls = state.lineSeparator != null ? state.lineSeparator : defaultLs;
        StringBuilder sb = new StringBuilder();

        for (LineEntry e : state.structure) {
            if (e == null) {
                continue;
            }
            appendStructuredLine(sb, e, data, ls);
        }

        return sb.toString();
    }

    private static void appendStructuredLine(StringBuilder sb, LineEntry e, Map<String, FileDataItem> data, String ls) {
        if (e.type == EntryType.EMPTY) {
            sb.append(ls);
            return;
        }

        if (e.type == EntryType.COMMENT || e.type == EntryType.RAW) {
            sb.append(e.text).append(ls);
            return;
        }

        if (e.type == EntryType.KEY) {
            FileDataItem item = data.get(e.key);
            if (item == null) {
                return;
            }
            String value = safeValue(item);
            sb.append(e.prefix).append(value).append(e.suffix).append(ls);
        }
    }

    private static String dumpFallback(Map<String, FileDataItem> data, String ls) {
        List<String> keys = new ArrayList<>();
        for (String k : data.keySet()) {
            if (META_KEY.equals(k)) {
                continue;
            }
            keys.add(k);
        }
        keys.sort(String::compareTo);

        StringBuilder sb = new StringBuilder();
        for (String k : keys) {
            FileDataItem item = data.get(k);
            if (item == null) {
                continue;
            }
            String v = safeValue(item);
            sb.append(k).append(": ").append(v).append(ls);
        }
        return sb.toString();
    }

    private static String safeValue(FileDataItem item) {
        Object v = item == null ? null : item.getValue();
        return v == null ? "" : String.valueOf(v);
    }

    private static String trimTrailingIfNeeded(String out, String ls) {
        if (out == null || out.isEmpty()) {
            return out;
        }
        if (ls != null && !ls.isEmpty() && out.endsWith(ls)) {
            return out.substring(0, out.length() - ls.length());
        }
        if (out.endsWith("\n") || out.endsWith("\r")) {
            return out.substring(0, out.length() - 1);
        }
        return out;
    }

    private static String detectLineSeparator(String data) {
        int idx = data.indexOf("\r\n");
        if (idx >= 0) {
            return "\r\n";
        }
        idx = data.indexOf('\n');
        if (idx >= 0) {
            return "\n";
        }
        idx = data.indexOf('\r');
        if (idx >= 0) {
            return "\r";
        }
        return "\n";
    }

    private static String encodeMeta(State st) {
        StringBuilder sb = new StringBuilder();
        sb.append(META_VERSION).append("\n");
        sb.append(st.originalEndsWithNewline ? "1" : "0").append("\n");
        sb.append(lineSepToken(st.lineSeparator)).append("\n");
        encodeEntries(sb, st.structure);
        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void encodeEntries(StringBuilder sb, List<LineEntry> entries) {
        for (LineEntry e : entries) {
            if (e == null) {
                continue;
            }
            sb.append(e.type.code).append("|");
            String payload = encodePayload(e);
            if (payload != null) {
                sb.append(Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8)));
            }
            sb.append("\n");
        }
    }

    private static String encodePayload(LineEntry e) {
        if (e.type == EntryType.KEY) {
            return nvl(e.key) + FIELD_SEP + nvl(e.prefix) + FIELD_SEP + nvl(e.suffix);
        }
        if (e.type == EntryType.COMMENT || e.type == EntryType.RAW) {
            return nvl(e.text);
        }
        return null;
    }

    private static State readMetaState(Map<String, FileDataItem> data) {
        FileDataItem metaItem = data.get(META_KEY);
        if (metaItem == null) {
            return null;
        }
        Object raw = metaItem.getValue();
        if (raw == null) {
            return null;
        }
        String b64 = String.valueOf(raw);
        if (b64.isEmpty()) {
            return null;
        }
        return decodeMeta(b64, DEFAULT_LS);
    }

    private static State decodeMeta(String b64, String defaultLs) {
        try {
            byte[] decoded = Base64.getDecoder().decode(b64);
            String text = new String(decoded, StandardCharsets.UTF_8);
            String[] lines = text.split("\\R", -1);
            if (lines.length < 3) {
                return null;
            }
            if (!META_VERSION.equals(lines[0])) {
                return null;
            }
            boolean endsWithNewline = "1".equals(lines[1]);
            String ls = tokenToLineSep(lines[2], defaultLs);
            State st = new State(endsWithNewline, ls, defaultLs);
            for (int i = 3; i < lines.length; i++) {
                LineEntry e = decodeEntry(lines[i]);
                if (e != null) {
                    st.structure.add(e);
                }
            }
            return st;
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static LineEntry decodeEntry(String line) {
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
        String payload = payloadB64.isEmpty()
                ? ""
                : new String(Base64.getDecoder().decode(payloadB64), StandardCharsets.UTF_8);

        if (type == EntryType.EMPTY) {
            return LineEntry.empty();
        }
        if (type == EntryType.COMMENT) {
            return LineEntry.comment(payload);
        }
        if (type == EntryType.RAW) {
            return LineEntry.raw(payload);
        }
        if (type == EntryType.KEY) {
            String[] parts = payload.split(FIELD_SEP, -1);
            String k = parts.length > 0 ? parts[0] : "";
            String p = parts.length > 1 ? parts[1] : "";
            String s = parts.length > 2 ? parts[2] : "";
            return LineEntry.forKey(k, p, s);
        }
        return null;
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

    private static String tokenToLineSep(String token, String defaultLs) {
        if ("CRLF".equals(token)) {
            return "\r\n";
        }
        if ("CR".equals(token)) {
            return "\r";
        }
        if ("LF".equals(token)) {
            return "\n";
        }
        return defaultLs;
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    private static final class ParseInput {
        final List<String> lines;
        final String lineSeparator;
        final boolean endsWithNewline;

        ParseInput(List<String> lines, String lineSeparator, boolean endsWithNewline) {
            this.lines = lines;
            this.lineSeparator = lineSeparator;
            this.endsWithNewline = endsWithNewline;
        }
    }

    private static final class ParseState {
        String currentKey;
        StringBuilder currentValue = new StringBuilder();
        String currentPrefix;
        String currentSuffix;
        final StringBuilder pendingMeta = new StringBuilder();
    }

    private static final class Range {
        final int start;
        final int end;

        Range(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    private static final class TopKeyParts {
        final String key;
        final String prefix;
        final String suffix;
        final String value;

        TopKeyParts(String key, String prefix, String suffix, String value) {
            this.key = key;
            this.prefix = prefix;
            this.suffix = suffix;
            this.value = value == null ? "" : value;
        }
    }

    private static final class State {
        final boolean originalEndsWithNewline;
        final String lineSeparator;
        final List<LineEntry> structure = new ArrayList<>();

        State(boolean originalEndsWithNewline, String lineSeparator, String defaultLs) {
            this.originalEndsWithNewline = originalEndsWithNewline;
            this.lineSeparator = lineSeparator != null ? lineSeparator : defaultLs;
        }
    }

    private enum EntryType {
        EMPTY('E'),
        COMMENT('C'),
        KEY('K'),
        RAW('R');

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

    private static final class LineEntry {
        final EntryType type;
        final String text;
        final String key;
        final String prefix;
        final String suffix;

        private LineEntry(EntryType type, String text, String key, String prefix, String suffix) {
            this.type = type;
            this.text = text;
            this.key = key;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        static LineEntry empty() {
            return new LineEntry(EntryType.EMPTY, null, null, null, null);
        }

        static LineEntry comment(String line) {
            return new LineEntry(EntryType.COMMENT, line, null, null, null);
        }

        static LineEntry raw(String line) {
            return new LineEntry(EntryType.RAW, line, null, null, null);
        }

        static LineEntry forKey(String key, String prefix, String suffix) {
            return new LineEntry(EntryType.KEY, null, key, prefix, suffix);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LineEntry other = (LineEntry) o;
            if (type != other.type) {
                return false;
            }
            if (!strEq(text, other.text)) {
                return false;
            }
            if (!strEq(key, other.key)) {
                return false;
            }
            if (!strEq(prefix, other.prefix)) {
                return false;
            }
            return strEq(suffix, other.suffix);
        }

        @Override
        public int hashCode() {
            int r = type == null ? 0 : type.hashCode();
            r = 31 * r + (text == null ? 0 : text.hashCode());
            r = 31 * r + (key == null ? 0 : key.hashCode());
            r = 31 * r + (prefix == null ? 0 : prefix.hashCode());
            r = 31 * r + (suffix == null ? 0 : suffix.hashCode());
            return r;
        }

        private static boolean strEq(String a, String b) {
            if (a == null) {
                return b == null;
            }
            return a.equals(b);
        }
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
        public int size() {
            int s = super.size();
            return super.containsKey(META_KEY) ? s - 1 : s;
        }

        @Override
        public boolean containsKey(Object key) {
            if (META_KEY.equals(key)) {
                return false;
            }
            return super.containsKey(key);
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }
    }
}