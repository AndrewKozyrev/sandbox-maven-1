import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

public class FlatIni implements FlatService {

    private static final String DEFAULT_LS = System.lineSeparator();

    private final Map<Map<String, FileDataItem>, State> stateByMap =
            Collections.synchronizedMap(new WeakHashMap<>());

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        Map<String, FileDataItem> result = new LinkedHashMap<>();

        if (data == null || data.isBlank()) {
            stateByMap.put(result, new State(new ArrayList<>(), DEFAULT_LS, false));
            return result;
        }

        String ls = detectLineSeparator(data);
        boolean endsWithNewline = endsWithAnyNewline(data);

        String[] all = data.split("\\R", -1);
        int n = all.length;
        if (endsWithNewline && n > 0 && all[n - 1].isEmpty()) {
            n--;
        }

        List<LineEntry> structure = new ArrayList<>();
        List<String> pendingItemComments = new ArrayList<>();

        boolean sawAnySection = false;
        String currentSection = null;

        Map<String, Integer> sectionNextIndex = new LinkedHashMap<>();
        Map<String, String> sectionPlaceholderKey = new LinkedHashMap<>();

        for (int i = 0; i < n; i++) {
            String line = all[i];
            if (line == null) {
                continue;
            }

            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                structure.add(LineEntry.empty());
                pendingItemComments.clear();
                continue;
            }

            if (startsWithCommentPrefix(trimmed)) {
                structure.add(LineEntry.comment(line));
                String summary = stripCommentPrefix(trimmed);
                if (!summary.isEmpty() && shouldAttachToNextItem(summary)) {
                    pendingItemComments.add(summary);
                }
                continue;
            }

            if (isSectionHeader(trimmed)) {
                sawAnySection = true;
                currentSection = extractSectionName(trimmed);
                structure.add(LineEntry.section(currentSection));
                pendingItemComments.clear();

                if (!sectionNextIndex.containsKey(currentSection)) {
                    sectionNextIndex.put(currentSection, 0);
                }

                if (!sectionPlaceholderKey.containsKey(currentSection)) {
                    String placeholderKey = currentSection + "[0]";
                    sectionPlaceholderKey.put(currentSection, placeholderKey);

                    if (!result.containsKey(placeholderKey)) {
                        FileDataItem ph = new FileDataItem();
                        ph.setKey(placeholderKey);
                        ph.setPath(placeholderKey);
                        ph.setValue("");
                        result.put(placeholderKey, ph);
                    }
                }
                continue;
            }

            if (!sawAnySection) {
                int eq = trimmed.indexOf('=');
                if (eq < 0) {
                    continue;
                }
                String key = trimmed.substring(0, eq).trim();
                String value = trimmed.substring(eq + 1).trim();

                FileDataItem item = new FileDataItem();
                item.setKey(key);
                item.setPath(key);
                item.setValue(value);

                String c = joinSummaryComments(pendingItemComments);
                if (c != null && !c.isEmpty()) {
                    item.setComment(c);
                }
                pendingItemComments.clear();

                result.put(key, item);
                structure.add(LineEntry.global(key));
                continue;
            }

            Integer idxObj = sectionNextIndex.get(currentSection);
            int idx = (idxObj == null) ? 0 : idxObj;
            sectionNextIndex.put(currentSection, idx + 1);

            String flatKey;
            String value;

            boolean hasWhitespace = containsWhitespace(trimmed);
            boolean isChildrenSection = currentSection.contains(":children");

            if (isChildrenSection || !hasWhitespace) {
                flatKey = currentSection + "[" + idx + "]";
                value = trimmed;
            } else {
                int ws = firstWhitespaceIndex(trimmed);
                String host = trimmed.substring(0, ws);
                String rest = trimmed.substring(ws).trim();
                flatKey = currentSection + "[" + idx + "]." + host;
                value = rest;
            }

            String placeholderKey = sectionPlaceholderKey.get(currentSection);
            if (idx == 0 && placeholderKey != null && !flatKey.equals(placeholderKey)) {
                result.remove(placeholderKey);
                sectionPlaceholderKey.put(currentSection, null);
            }

            FileDataItem item = new FileDataItem();
            item.setKey(flatKey);
            item.setPath(flatKey);
            item.setValue(value);

            String c = joinSummaryComments(pendingItemComments);
            if (c != null && !c.isEmpty()) {
                item.setComment(c);
            }
            pendingItemComments.clear();

            result.put(flatKey, item);
            structure.add(LineEntry.data(flatKey));
        }

        stateByMap.put(result, new State(structure, ls, endsWithNewline));
        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        State state = stateByMap.get(data);
        String ls = (state == null || state.lineSeparator == null) ? DEFAULT_LS : state.lineSeparator;
        boolean endsWithNewline = state != null && state.endsWithNewline;
        List<LineEntry> structure = state == null ? null : state.structure;

        if (structure == null || structure.isEmpty()) {
            List<String> keys = new ArrayList<>(data.keySet());
            Collections.sort(keys);

            StringBuilder sb = new StringBuilder();
            for (String k : keys) {
                FileDataItem item = data.get(k);
                if (item == null || item.getKey() == null) {
                    continue;
                }

                String comment = item.getComment();
                if (comment != null && !comment.isBlank()) {
                    String[] cl = comment.split("\\R");
                    for (String s : cl) {
                        if (s != null && !s.isBlank()) {
                            sb.append("#").append(s.trim()).append(ls);
                        }
                    }
                }

                String v = item.getValue() == null ? "" : item.getValue().toString();
                sb.append(item.getKey()).append("=").append(v);
                sb.append(ls);
            }

            String res = sb.toString();
            return trimTrailingNewline(res, ls, endsWithNewline);
        }

        StringBuilder sb = new StringBuilder();
        for (LineEntry e : structure) {
            if (e == null) {
                continue;
            }

            if (e.type == LineType.EMPTY) {
                sb.append(ls);
                continue;
            }

            if (e.type == LineType.COMMENT) {
                sb.append(e.text == null ? "" : e.text).append(ls);
                continue;
            }

            if (e.type == LineType.SECTION) {
                sb.append("[").append(e.text == null ? "" : e.text).append("]").append(ls);
                continue;
            }

            if (e.type == LineType.GLOBAL) {
                String key = e.text;
                FileDataItem item = key == null ? null : data.get(key);
                if (item == null || item.getKey() == null) {
                    continue;
                }
                String v = item.getValue() == null ? "" : item.getValue().toString();
                sb.append(item.getKey()).append("=").append(v).append(ls);
                continue;
            }

            if (e.type == LineType.DATA) {
                String flatKey = e.text;
                FileDataItem item = flatKey == null ? null : data.get(flatKey);
                if (item == null || item.getKey() == null) {
                    continue;
                }
                sb.append(formatDataLine(flatKey, item)).append(ls);
            }
        }

        String res = sb.toString();
        return trimTrailingNewline(res, ls, endsWithNewline);
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new UnsupportedOperationException("Validation of INI files is not implemented.");
    }

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.length() >= 2;
    }

    private static String extractSectionName(String trimmed) {
        return trimmed.substring(1, trimmed.length() - 1).trim();
    }

    private static boolean startsWithCommentPrefix(String trimmed) {
        return trimmed.startsWith("#") || trimmed.startsWith(";");
    }

    private static String stripCommentPrefix(String trimmed) {
        if (trimmed.length() <= 1) {
            return "";
        }
        return trimmed.substring(1).trim();
    }

    private static boolean shouldAttachToNextItem(String summary) {
        if (summary == null) {
            return false;
        }
        String s = summary.trim();
        if (s.isEmpty()) {
            return false;
        }
        if (s.contains("ansible_")) {
            return false;
        }
        if (s.contains("=")) {
            return false;
        }
        if (s.indexOf('.') >= 0 && containsWhitespace(s)) {
            return false;
        }
        if (s.indexOf('.') >= 0 && s.length() > 20) {
            return false;
        }
        return true;
    }

    private static String joinSummaryComments(List<String> comments) {
        if (comments == null || comments.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String s : comments) {
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
        return sb.length() == 0 ? null : sb.toString();
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

    private static String formatDataLine(String flatKey, FileDataItem item) {
        String value = item.getValue() == null ? "" : item.getValue().toString();
        int dotAfterBracket = flatKey.indexOf("].");
        if (dotAfterBracket >= 0) {
            String host = flatKey.substring(dotAfterBracket + 2);
            if (value.isEmpty()) {
                return host;
            }
            return host + " " + value;
        }
        return value;
    }

    private static boolean endsWithAnyNewline(String data) {
        if (data == null || data.isEmpty()) {
            return false;
        }
        char c = data.charAt(data.length() - 1);
        return c == '\n' || c == '\r';
    }

    private static String detectLineSeparator(String data) {
        if (data == null || data.isEmpty()) {
            return DEFAULT_LS;
        }
        int rn = data.indexOf("\r\n");
        if (rn >= 0) {
            return "\r\n";
        }
        int n = data.indexOf('\n');
        if (n >= 0) {
            return "\n";
        }
        int r = data.indexOf('\r');
        if (r >= 0) {
            return "\r";
        }
        return DEFAULT_LS;
    }

    private static String trimTrailingNewline(String s, String ls, boolean keepTrailing) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        if (keepTrailing) {
            return s;
        }
        if (ls != null && !ls.isEmpty() && s.endsWith(ls)) {
            return s.substring(0, s.length() - ls.length());
        }
        if (s.endsWith("\n") || s.endsWith("\r")) {
            return s.substring(0, s.length() - 1);
        }
        return s;
    }

    private enum LineType {
        EMPTY,
        COMMENT,
        SECTION,
        DATA,
        GLOBAL
    }

    private static final class LineEntry {
        final LineType type;
        final String text;

        private LineEntry(LineType type, String text) {
            this.type = type;
            this.text = text;
        }

        static LineEntry empty() {
            return new LineEntry(LineType.EMPTY, null);
        }

        static LineEntry comment(String rawLine) {
            return new LineEntry(LineType.COMMENT, rawLine);
        }

        static LineEntry section(String sectionName) {
            return new LineEntry(LineType.SECTION, sectionName);
        }

        static LineEntry data(String flatKey) {
            return new LineEntry(LineType.DATA, flatKey);
        }

        static LineEntry global(String key) {
            return new LineEntry(LineType.GLOBAL, key);
        }
    }

    private static final class State {
        final List<LineEntry> structure;
        final String lineSeparator;
        final boolean endsWithNewline;

        private State(List<LineEntry> structure, String lineSeparator, boolean endsWithNewline) {
            this.structure = structure;
            this.lineSeparator = lineSeparator;
            this.endsWithNewline = endsWithNewline;
        }
    }
}