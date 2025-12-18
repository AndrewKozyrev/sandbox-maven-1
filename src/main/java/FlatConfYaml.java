import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatConfYaml implements FlatService {

    private static final String DEFAULT_LS = "\n";

    private static final Object CACHE_LOCK = new Object();
    private static final IdentityHashMap<Map<String, FileDataItem>, State> STATE_BY_MAP = new IdentityHashMap<>();
    private static final IdentityHashMap<FileDataItem, State> STATE_BY_ITEM = new IdentityHashMap<>();
    private static final int MAX_MAP_CACHE = 256;
    private static final int MAX_ITEM_CACHE = 8192;

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        StructuredMap result = new StructuredMap();
        if (data == null || data.isBlank()) {
            return result;
        }

        String lineSeparator = detectLineSeparator(data);
        boolean endsWithNewline = data.endsWith("\n") || data.endsWith("\r");

        String[] raw = data.split("\\R", -1);
        List<String> lines = new ArrayList<>();
        Collections.addAll(lines, raw);
        if (endsWithNewline && !lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
            lines.remove(lines.size() - 1);
        }

        State st = new State(endsWithNewline, lineSeparator);
        result.state = st;

        parse(lines, result, st);

        rememberState(result, st);
        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        State st = resolveState(data);
        if (st == null || st.structure.isEmpty()) {
            return dumpFallback(data, DEFAULT_LS);
        }

        String out = dumpWithStructure(data, st);

        if (!st.originalEndsWithNewline) {
            out = trimTrailingIfNeeded(out, st.lineSeparator);
        }
        return out;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation of .conf.yml files is not implemented yet.");
    }

    private static void parse(List<String> lines, LinkedHashMap<String, FileDataItem> result, State st) {
        String currentKey = null;
        StringBuilder currentValue = new StringBuilder();
        String currentPrefix = null;
        String currentSuffix = null;

        StringBuilder pendingMeta = new StringBuilder();

        for (String line : lines) {
            if (line == null) {
                continue;
            }

            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                st.structure.add(LineEntry.empty());
                // separator: meta comments shouldn't cross blank lines
                pendingMeta.setLength(0);
                continue;
            }

            if (trimmed.startsWith("#")) {
                st.structure.add(LineEntry.comment(line));
                if (pendingMeta.length() > 0) {
                    pendingMeta.append('\n');
                }
                pendingMeta.append(trimmed.substring(1).trim());
                continue;
            }

            // continuation of multiline value (indented lines, lists, etc)
            if (currentKey != null && (line.startsWith(" ") || line.startsWith("\t") || trimmed.startsWith("- "))) {
                currentValue.append('\n').append(line);
                continue;
            }

            // new top-level key => commit previous
            if (isTopLevelKeyLine(line)) {
                commitCurrent(result, currentKey, currentValue, pendingMeta, st, currentPrefix, currentSuffix);

                int colonIdx = line.indexOf(':');
                String keyRaw = line.substring(0, colonIdx);
                String key = keyRaw.trim();

                String afterColon = line.substring(colonIdx + 1);
                int commentIdx = findInlineCommentIndex(afterColon);

                String nonCommentPart = commentIdx >= 0 ? afterColon.substring(0, commentIdx) : afterColon;
                String commentPart = commentIdx >= 0 ? afterColon.substring(commentIdx) : "";

                int valueStart = 0;
                while (valueStart < nonCommentPart.length() && Character.isWhitespace(nonCommentPart.charAt(valueStart))) {
                    valueStart++;
                }
                int valueEnd = nonCommentPart.length();
                while (valueEnd > valueStart && Character.isWhitespace(nonCommentPart.charAt(valueEnd - 1))) {
                    valueEnd--;
                }

                currentPrefix = line.substring(0, colonIdx + 1) + nonCommentPart.substring(0, valueStart);
                currentSuffix = nonCommentPart.substring(valueEnd) + commentPart;

                String valueText = nonCommentPart.substring(valueStart, valueEnd).trim();

                currentKey = key;
                currentValue = new StringBuilder();
                currentValue.append(valueText);

                st.structure.add(LineEntry.key(currentKey, currentPrefix, currentSuffix));
                continue;
            }

            // unknown line (should be rare in this simplified parser) - preserve it in output
            st.structure.add(LineEntry.raw(line));
            pendingMeta.setLength(0);
        }

        commitCurrent(result, currentKey, currentValue, pendingMeta, st, currentPrefix, currentSuffix);
    }

    private static void commitCurrent(
            LinkedHashMap<String, FileDataItem> result,
            String currentKey,
            StringBuilder currentValue,
            StringBuilder pendingMeta,
            State st,
            String currentPrefix,
            String currentSuffix
    ) {
        if (currentKey == null) {
            return;
        }

        FileDataItem item = new FileDataItem();
        item.setKey(currentKey);
        item.setPath(currentKey);
        item.setValue(currentValue.toString());

        if (pendingMeta.length() > 0) {
            item.setComment(pendingMeta.toString());
        }

        result.put(currentKey, item);

        // meta belongs to the key we just committed
        pendingMeta.setLength(0);
    }

    private static boolean isTopLevelKeyLine(String line) {
        if (line == null) return false;
        if (line.isEmpty()) return false;
        char c0 = line.charAt(0);
        if (c0 == ' ' || c0 == '\t') return false; // indented => continuation
        if (line.startsWith("- ")) return false; // list item
        int idx = line.indexOf(':');
        return idx > 0; // key:
    }

    private static int findInlineCommentIndex(String s) {
        if (s == null || s.isEmpty()) return -1;

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

    private static String dumpWithStructure(Map<String, FileDataItem> data, State st) {
        String ls = st.lineSeparator != null ? st.lineSeparator : DEFAULT_LS;
        StringBuilder sb = new StringBuilder();

        for (LineEntry e : st.structure) {
            if (e.type == LineType.EMPTY) {
                sb.append(ls);
                continue;
            }
            if (e.type == LineType.COMMENT || e.type == LineType.RAW) {
                sb.append(e.text).append(ls);
                continue;
            }
            if (e.type == LineType.KEY) {
                FileDataItem item = data.get(e.key);
                if (item == null) {
                    continue; // key removed
                }
                String value = item.getValue() == null ? "" : item.getValue().toString();
                // value is scalar for this simplified format
                sb.append(e.prefix).append(value).append(e.suffix).append(ls);
            }
        }

        return sb.toString();
    }

    private static String dumpFallback(Map<String, FileDataItem> data, String ls) {
        // deterministic: sort keys
        List<String> keys = new ArrayList<>(data.keySet());
        keys.sort(String::compareTo);

        StringBuilder sb = new StringBuilder();
        for (String k : keys) {
            FileDataItem item = data.get(k);
            if (item == null) continue;
            String v = item.getValue() == null ? "" : item.getValue().toString();
            sb.append(k).append(": ").append(v).append(ls);
        }
        return sb.toString();
    }

    private static String trimTrailingIfNeeded(String out, String ls) {
        if (out == null || out.isEmpty()) return out;
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
        if (idx >= 0) return "\r\n";
        idx = data.indexOf('\n');
        if (idx >= 0) return "\n";
        idx = data.indexOf('\r');
        if (idx >= 0) return "\r";
        return DEFAULT_LS;
    }

    private static void rememberState(Map<String, FileDataItem> map, State st) {
        synchronized (CACHE_LOCK) {
            if (STATE_BY_MAP.size() > MAX_MAP_CACHE) {
                STATE_BY_MAP.clear();
            }
            if (STATE_BY_ITEM.size() > MAX_ITEM_CACHE) {
                STATE_BY_ITEM.clear();
            }
            STATE_BY_MAP.put(map, st);
            for (FileDataItem item : map.values()) {
                if (item != null) {
                    STATE_BY_ITEM.put(item, st);
                }
            }
        }
    }

    private static State resolveState(Map<String, FileDataItem> data) {
        if (data instanceof StructuredMap) {
            State st = ((StructuredMap) data).state;
            if (st != null) return st;
        }

        synchronized (CACHE_LOCK) {
            State st = STATE_BY_MAP.get(data);
            if (st != null) return st;
        }

        synchronized (CACHE_LOCK) {
            for (FileDataItem item : data.values()) {
                if (item == null) continue;
                State st = STATE_BY_ITEM.get(item);
                if (st != null) return st;
            }
        }

        return null;
    }

    private static final class StructuredMap extends LinkedHashMap<String, FileDataItem> {
        private State state;
    }

    private static final class State {
        final boolean originalEndsWithNewline;
        final String lineSeparator;
        final List<LineEntry> structure = new ArrayList<>();

        State(boolean originalEndsWithNewline, String lineSeparator) {
            this.originalEndsWithNewline = originalEndsWithNewline;
            this.lineSeparator = lineSeparator != null ? lineSeparator : DEFAULT_LS;
        }
    }

    private enum LineType { EMPTY, COMMENT, KEY, RAW }

    private static final class LineEntry {
        final LineType type;
        final String text;   // for COMMENT/RAW
        final String key;    // for KEY
        final String prefix; // for KEY
        final String suffix; // for KEY

        private LineEntry(LineType type, String text, String key, String prefix, String suffix) {
            this.type = type;
            this.text = text;
            this.key = key;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        static LineEntry empty() {
            return new LineEntry(LineType.EMPTY, null, null, null, null);
        }

        static LineEntry comment(String line) {
            return new LineEntry(LineType.COMMENT, line, null, null, null);
        }

        static LineEntry raw(String line) {
            return new LineEntry(LineType.RAW, line, null, null, null);
        }

        static LineEntry key(String key, String prefix, String suffix) {
            return new LineEntry(LineType.KEY, null, key, prefix, suffix);
        }
    }
}