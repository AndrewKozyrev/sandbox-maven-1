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
    private static final String META_VERSION = "V2";
    private static final String FIELD_SEP = "\u0001";

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        MetaHidingMap result = new MetaHidingMap();
        if (data == null || data.isBlank()) {
            return result;
        }

        ParseInput input = ParseInput.from(data);
        State st = new State(input.endsWithNewline, input.lineSeparator);
        ParseState ps = new ParseState();

        for (String line : input.lines) {
            processInputLine(line, result, st, ps);
        }

        commitCurrent(result, ps);
        result.putInternal(buildMetaItem(st));
        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        State st = readMetaState(data);
        String out;

        if (st == null || st.structure.isEmpty()) {
            out = dumpFallback(data);
        } else {
            out = dumpWithStructure(data, st);
            if (!st.originalEndsWithNewline) {
                out = trimTrailingLineSep(out, st.lineSeparator);
            }
        }

        return out;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation of .conf.yml files is not implemented yet.");
    }

    private static FileDataItem buildMetaItem(State st) {
        FileDataItem metaItem = new FileDataItem();
        metaItem.setKey(META_KEY);
        metaItem.setComment(encodeMeta(st));
        return metaItem;
    }

    private static void processInputLine(String line, Map<String, FileDataItem> result, State st, ParseState ps) {
        if (line == null) {
            return;
        }

        LineKind kind = classifyLine(line, ps);
        switch (kind) {
            case EMPTY:
                commitCurrent(result, ps);
                st.structure.add(LineEntry.empty());
                ps.clearPendingComments();
                break;
            case COMMENT:
                commitCurrent(result, ps);
                st.structure.add(LineEntry.comment(line));
                ps.pendingCommentLines.add(extractCommentText(line));
                break;
            case CONTINUATION:
                ps.currentValue.append('\n').append(line);
                break;
            case KEY:
                handleKeyLine(line, result, st, ps);
                break;
            case RAW:
            default:
                commitCurrent(result, ps);
                st.structure.add(LineEntry.raw(line));
                ps.clearPendingComments();
                break;
        }
    }

    private static LineKind classifyLine(String line, ParseState ps) {
        if (isEmptyLine(line)) {
            return LineKind.EMPTY;
        }
        if (isCommentLine(line)) {
            return LineKind.COMMENT;
        }
        if (isContinuationLine(line, ps)) {
            return LineKind.CONTINUATION;
        }
        if (isTopLevelKeyLine(line)) {
            return LineKind.KEY;
        }
        return LineKind.RAW;
    }

    private static boolean isContinuationLine(String line, ParseState ps) {
        if (ps.currentKey == null) {
            return false;
        }
        String trimmed = line.trim();
        boolean indented = startsWithIndent(line);
        boolean listItem = trimmed.startsWith("- ");
        return indented || listItem;
    }

    private static void handleKeyLine(String line, Map<String, FileDataItem> result, State st, ParseState ps) {
        commitCurrent(result, ps);

        TopKeyParts parts = splitTopLevelKey(line);
        if (parts == null) {
            st.structure.add(LineEntry.raw(line));
            ps.clearPendingComments();
            return;
        }

        String leadingComment = joinPendingComments(ps.pendingCommentLines);
        int originalCommentCount = ps.pendingCommentLines.size();
        ps.clearPendingComments();

        ps.currentKey = parts.key;
        ps.currentPrefix = parts.prefix;
        ps.currentSuffix = parts.suffix;
        ps.currentValue = new StringBuilder(parts.value);
        ps.currentLeadingComment = leadingComment;

        st.structure.add(LineEntry.forKey(ps.currentKey, ps.currentPrefix, ps.currentSuffix, originalCommentCount));
    }

    private static void commitCurrent(Map<String, FileDataItem> result, ParseState ps) {
        if (ps.currentKey == null) {
            return;
        }

        FileDataItem item = new FileDataItem();
        item.setKey(ps.currentKey);
        item.setValue(ps.currentValue == null ? "" : ps.currentValue.toString());

        if (ps.currentLeadingComment != null && !ps.currentLeadingComment.isEmpty()) {
            item.setComment(ps.currentLeadingComment);
        }

        result.put(ps.currentKey, item);
        ps.resetCurrent();
    }

    private static String dumpWithStructure(Map<String, FileDataItem> data, State st) {
        DumpContext ctx = new DumpContext(data, st.lineSeparator);
        int idx = 0;
        while (idx < st.structure.size()) {
            idx = processStructureEntry(ctx, st.structure, idx);
        }
        appendKeysNotInStructure(ctx);
        return ctx.sb.toString();
    }

    private static int processStructureEntry(DumpContext ctx, List<LineEntry> structure, int idx) {
        LineEntry e = structure.get(idx);
        if (e == null) {
            return idx + 1;
        }

        EntryType type = e.type;
        if (type == EntryType.EMPTY) {
            ctx.sb.append(ctx.ls);
            return idx + 1;
        }
        if (type == EntryType.RAW) {
            ctx.sb.append(nvl(e.text)).append(ctx.ls);
            return idx + 1;
        }
        if (type == EntryType.KEY) {
            appendKeyWithoutLeadingComments(ctx, e);
            return idx + 1;
        }
        if (type == EntryType.COMMENT) {
            int end = commentBlockEnd(structure, idx);
            if (end < structure.size()) {
                LineEntry next = structure.get(end);
                if (next != null && next.type == EntryType.KEY) {
                    appendKeyWithLeadingComments(ctx, structure, idx, end, next);
                    return end + 1;
                }
            }
            appendCommentRangeAsIs(ctx, structure, idx, end);
            return end;
        }
        return idx + 1;
    }

    private static int commentBlockEnd(List<LineEntry> structure, int start) {
        int idx = start;
        while (idx < structure.size()) {
            LineEntry e = structure.get(idx);
            if (e == null || e.type != EntryType.COMMENT) {
                return idx;
            }
            idx++;
        }
        return idx;
    }

    private static void appendCommentRangeAsIs(DumpContext ctx, List<LineEntry> structure, int start, int end) {
        for (LineEntry ce : structure.subList(start, end)) {
            if (ce != null) {
                ctx.sb.append(nvl(ce.text)).append(ctx.ls);
            }
        }
    }

    private static void appendKeyWithoutLeadingComments(DumpContext ctx, LineEntry e) {
        if (e.key != null) {
            ctx.seenKeys.add(e.key);
        }

        FileDataItem item = ctx.data.get(e.key);
        if (item == null) {
            return;
        }

        appendInsertedComment(ctx, item.getComment(), "");
        ctx.sb.append(nvl(e.prefix)).append(safeValue(item)).append(nvl(e.suffix)).append(ctx.ls);
    }

    private static void appendKeyWithLeadingComments(
            DumpContext ctx,
            List<LineEntry> structure,
            int commentStart,
            int commentEnd,
            LineEntry keyEntry
    ) {
        if (keyEntry.key != null) {
            ctx.seenKeys.add(keyEntry.key);
        }

        FileDataItem item = ctx.data.get(keyEntry.key);
        if (item == null) {
            return;
        }

        String newComment = item.getComment();
        String originalComment = buildOriginalCommentText(structure, commentStart, commentEnd);

        if (newComment != null && !newComment.isBlank()) {
            if (newComment.equals(originalComment)) {
                appendCommentRangeAsIs(ctx, structure, commentStart, commentEnd);
            } else {
                String indent = findCommentIndent(structure, commentStart, commentEnd);
                appendInsertedComment(ctx, newComment, indent);
            }
        }

        ctx.sb.append(nvl(keyEntry.prefix)).append(safeValue(item)).append(nvl(keyEntry.suffix)).append(ctx.ls);
    }

    private static String buildOriginalCommentText(List<LineEntry> structure, int start, int end) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (LineEntry e : structure.subList(start, end)) {
            if (e != null && e.type == EntryType.COMMENT) {
                if (!first) {
                    sb.append('\n');
                }
                sb.append(extractCommentText(nvl(e.text)));
                first = false;
            }
        }
        return first ? null : sb.toString();
    }

    private static String findCommentIndent(List<LineEntry> structure, int start, int end) {
        for (LineEntry e : structure.subList(start, end)) {
            if (e != null && e.type == EntryType.COMMENT) {
                String t = nvl(e.text);
                int idx = t.indexOf('#');
                return idx >= 0 ? t.substring(0, idx) : "";
            }
        }
        return "";
    }

    private static void appendKeysNotInStructure(DumpContext ctx) {
        for (Map.Entry<String, FileDataItem> en : ctx.data.entrySet()) {
            String k = en.getKey();
            if (k == null || META_KEY.equals(k) || ctx.seenKeys.contains(k)) {
                continue;
            }
            appendNewKey(ctx, k, en.getValue());
        }
    }

    private static String dumpFallback(Map<String, FileDataItem> data) {
        DumpContext ctx = new DumpContext(data, DEFAULT_LS);
        for (Map.Entry<String, FileDataItem> en : data.entrySet()) {
            String k = en.getKey();
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            appendNewKey(ctx, k, en.getValue());
        }
        return ctx.sb.toString();
    }

    private static void appendNewKey(DumpContext ctx, String key, FileDataItem item) {
        if (item == null) {
            return;
        }
        appendInsertedComment(ctx, item.getComment(), "");
        ctx.sb.append(key).append(": ").append(safeValue(item)).append(ctx.ls);
    }

    private static void appendInsertedComment(DumpContext ctx, String comment, String indentBeforeHash) {
        if (comment == null || comment.isBlank()) {
            return;
        }
        String prefix = indentBeforeHash == null ? "" : indentBeforeHash;
        String[] lines = comment.split("\\R", -1);
        for (String line : lines) {
            if (line != null) {
                ctx.sb.append(prefix).append("#").append(line).append(ctx.ls);
            }
        }
    }

    private static boolean isEmptyLine(String line) {
        return line.trim().isEmpty();
    }

    private static boolean isCommentLine(String line) {
        String t = line.trim();
        return !t.isEmpty() && t.charAt(0) == '#';
    }

    private static String extractCommentText(String line) {
        int idx = line.indexOf('#');
        if (idx < 0) {
            return "";
        }
        return line.substring(idx + 1);
    }

    private static String joinPendingComments(List<String> pending) {
        if (pending == null || pending.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String line : pending) {
            if (!first) {
                sb.append('\n');
            }
            sb.append(nvl(line));
            first = false;
        }
        return sb.toString();
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

    private static boolean startsWithIndent(String line) {
        if (line.isEmpty()) {
            return false;
        }
        char c0 = line.charAt(0);
        return c0 == ' ' || c0 == '\t';
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

        String afterColon = (colonIdx + 1 < line.length()) ? line.substring(colonIdx + 1) : "";
        int commentIdx = findInlineCommentIndex(afterColon);

        String nonCommentPart = commentIdx >= 0 ? afterColon.substring(0, commentIdx) : afterColon;
        String commentPart = commentIdx >= 0 ? afterColon.substring(commentIdx) : "";

        Range r = trimRange(nonCommentPart);

        String valueText;
        String prefixSpaces;
        String suffixSpaces;

        if (r == null) {
            valueText = "";
            prefixSpaces = nonCommentPart;
            suffixSpaces = "";
        } else {
            valueText = nonCommentPart.substring(r.start, r.end).trim();
            prefixSpaces = nonCommentPart.substring(0, r.start);
            suffixSpaces = nonCommentPart.substring(r.end);
        }

        String prefix = line.substring(0, colonIdx + 1) + prefixSpaces;
        String suffix = suffixSpaces + commentPart;

        return new TopKeyParts(key, prefix, suffix, valueText);
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

    private static String safeValue(FileDataItem item) {
        Object v = item == null ? null : item.getValue();
        return v == null ? "" : String.valueOf(v);
    }

    private static String trimTrailingLineSep(String out, String ls) {
        if (out == null || out.isEmpty()) {
            return out;
        }
        if (ls != null && !ls.isEmpty() && out.endsWith(ls)) {
            return out.substring(0, out.length() - ls.length());
        }
        int end = out.length();
        while (end > 0) {
            char ch = out.charAt(end - 1);
            if (ch == '\n' || ch == '\r') {
                end--;
            } else {
                return out;
            }
        }
        return out.substring(0, end);
    }

    private static String encodeMeta(State st) {
        StringBuilder sb = new StringBuilder();
        sb.append(META_VERSION).append('\n');
        sb.append(st.originalEndsWithNewline ? "1" : "0").append('\n');
        sb.append(lineSepToken(st.lineSeparator)).append('\n');

        for (LineEntry e : st.structure) {
            if (e == null) {
                continue;
            }
            sb.append(e.type.code).append('|');
            String payload = encodePayload(e);
            if (payload != null) {
                sb.append(Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8)));
            }
            sb.append('\n');
        }

        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String encodePayload(LineEntry e) {
        if (e.type == EntryType.KEY) {
            return nvl(e.key) + FIELD_SEP + nvl(e.prefix) + FIELD_SEP + nvl(e.suffix) + FIELD_SEP + e.originalCommentCount;
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

        String b64 = metaItem.getComment();
        if (b64 == null || b64.isEmpty()) {
            Object raw = metaItem.getValue();
            if (raw == null) {
                return null;
            }
            b64 = String.valueOf(raw);
        }

        if (b64.isEmpty()) {
            return null;
        }

        return decodeMeta(b64);
    }

    private static State decodeMeta(String b64) {
        try {
            byte[] decoded = Base64.getDecoder().decode(b64);
            String text = new String(decoded, StandardCharsets.UTF_8);
            return parseMetaText(text);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static State parseMetaText(String text) {
        String[] lines = text.split("\\R", -1);
        if (lines.length < 3) {
            return null;
        }
        if (!META_VERSION.equals(lines[0])) {
            return null;
        }

        boolean endsWithNewline = "1".equals(lines[1]);
        String ls = tokenToLineSep(lines[2]);
        State st = new State(endsWithNewline, ls);

        for (int i = 3; i < lines.length; i++) {
            LineEntry e = decodeEntry(lines[i]);
            if (e != null) {
                st.structure.add(e);
            }
        }

        return st;
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

        String payload = decodePayload(line.substring(bar + 1));

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
            return decodeKeyEntry(payload);
        }

        return null;
    }

    private static LineEntry decodeKeyEntry(String payload) {
        String[] parts = payload.split(FIELD_SEP, -1);
        String k = parts.length > 0 ? parts[0] : "";
        String p = parts.length > 1 ? parts[1] : "";
        String s = parts.length > 2 ? parts[2] : "";
        int c = parseIntSafe(parts.length > 3 ? parts[3] : "0");
        return LineEntry.forKey(k, p, s, c);
    }

    private static int parseIntSafe(String s) {
        if (s == null || s.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    private static String decodePayload(String payloadB64) {
        if (payloadB64 == null || payloadB64.isEmpty()) {
            return "";
        }
        return new String(Base64.getDecoder().decode(payloadB64), StandardCharsets.UTF_8);
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

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    private enum LineKind {
        EMPTY,
        COMMENT,
        CONTINUATION,
        KEY,
        RAW
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

    private static final class DumpContext {
        final Map<String, FileDataItem> data;
        final String ls;
        final StringBuilder sb = new StringBuilder();
        final Set<String> seenKeys = new LinkedHashSet<>();

        private DumpContext(Map<String, FileDataItem> data, String ls) {
            this.data = data;
            this.ls = ls == null ? DEFAULT_LS : ls;
        }
    }

    private static final class ParseInput {
        final List<String> lines;
        final String lineSeparator;
        final boolean endsWithNewline;

        private ParseInput(List<String> lines, String lineSeparator, boolean endsWithNewline) {
            this.lines = lines;
            this.lineSeparator = lineSeparator;
            this.endsWithNewline = endsWithNewline;
        }

        static ParseInput from(String data) {
            String ls = detectLineSeparator(data);
            boolean ends = data.endsWith("\n") || data.endsWith("\r");

            String[] raw = data.split("\\R", -1);
            List<String> lines = new ArrayList<>(raw.length);
            Collections.addAll(lines, raw);

            if (ends && !lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
                lines.remove(lines.size() - 1);
            }

            return new ParseInput(lines, ls, ends);
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
            return DEFAULT_LS;
        }
    }

    private static final class ParseState {
        String currentKey;
        StringBuilder currentValue = new StringBuilder();
        String currentPrefix;
        String currentSuffix;
        String currentLeadingComment;
        final List<String> pendingCommentLines = new ArrayList<>();

        void clearPendingComments() {
            pendingCommentLines.clear();
        }

        void resetCurrent() {
            currentKey = null;
            currentPrefix = null;
            currentSuffix = null;
            currentValue = new StringBuilder();
            currentLeadingComment = null;
        }
    }

    private static final class Range {
        final int start;
        final int end;

        private Range(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    private static final class TopKeyParts {
        final String key;
        final String prefix;
        final String suffix;
        final String value;

        private TopKeyParts(String key, String prefix, String suffix, String value) {
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

        private State(boolean originalEndsWithNewline, String lineSeparator) {
            this.originalEndsWithNewline = originalEndsWithNewline;
            this.lineSeparator = lineSeparator == null ? DEFAULT_LS : lineSeparator;
        }
    }

    private static final class LineEntry {
        final EntryType type;
        final String text;
        final String key;
        final String prefix;
        final String suffix;
        final int originalCommentCount;

        private LineEntry(EntryType type, String text, String key, String prefix, String suffix, int originalCommentCount) {
            this.type = type;
            this.text = text;
            this.key = key;
            this.prefix = prefix;
            this.suffix = suffix;
            this.originalCommentCount = originalCommentCount;
        }

        static LineEntry empty() {
            return new LineEntry(EntryType.EMPTY, null, null, null, null, 0);
        }

        static LineEntry comment(String line) {
            return new LineEntry(EntryType.COMMENT, line, null, null, null, 0);
        }

        static LineEntry raw(String line) {
            return new LineEntry(EntryType.RAW, line, null, null, null, 0);
        }

        static LineEntry forKey(String key, String prefix, String suffix, int originalCommentCount) {
            return new LineEntry(EntryType.KEY, null, key, prefix, suffix, originalCommentCount);
        }
    }

    private static final class MetaHidingMap extends LinkedHashMap<String, FileDataItem> {

        void putInternal(FileDataItem value) {
            super.put(META_KEY, value);
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