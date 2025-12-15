import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatIni implements FlatService {

    private final List<LineEntry> structure = new ArrayList<>();

    private boolean originalEndsWithNewline = false;
    private String originalLineSeparator = "\n";

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        Map<String, FileDataItem> result = new LinkedHashMap<>();
        structure.clear();
        originalEndsWithNewline = false;
        originalLineSeparator = "\n";

        if (data == null || data.isBlank()) {
            return result;
        }

        originalEndsWithNewline = data.endsWith("\n") || data.endsWith("\r");
        originalLineSeparator = detectLineSeparator(data);

        String[] lines = data.split("\\R", -1);
        if (originalEndsWithNewline && lines.length > 0 && lines[lines.length - 1].isEmpty()) {
            lines = Arrays.copyOf(lines, lines.length - 1);
        }

        String currentSection = null;
        int currentSectionCount = 0;

        Map<String, Integer> sectionNextIndex = new LinkedHashMap<>();
        List<String> pendingSummaryComments = new ArrayList<>();

        for (String line : lines) {
            if (line == null) {
                continue;
            }

            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                structure.add(LineEntry.empty());
                continue;
            }

            if (isCommentLine(trimmed)) {
                structure.add(LineEntry.comment(line));
                String summary = toSummaryComment(trimmed);
                if (summary != null && !summary.isEmpty()) {
                    pendingSummaryComments.add(summary);
                }
                continue;
            }

            if (isSectionHeader(trimmed)) {
                if (currentSection != null && currentSectionCount == 0) {
                    String phKey = currentSection + "[0]";
                    if (!result.containsKey(phKey)) {
                        FileDataItem ph = new FileDataItem();
                        ph.setKey(phKey);
                        ph.setPath(phKey);
                        ph.setValue("");
                        result.put(phKey, ph);
                    }
                }

                currentSection = trimmed.substring(1, trimmed.length() - 1).trim();
                currentSectionCount = 0;
                structure.add(LineEntry.section(currentSection));
                pendingSummaryComments.clear();
                continue;
            }

            if (currentSection == null) {
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

                String c = joinSummary(pendingSummaryComments);
                if (c != null) {
                    item.setComment(c);
                }
                pendingSummaryComments.clear();

                result.put(key, item);
                structure.add(LineEntry.globalData(key));
                continue;
            }

            int idx = sectionNextIndex.getOrDefault(currentSection, 0);
            sectionNextIndex.put(currentSection, idx + 1);
            currentSectionCount++;

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

            FileDataItem item = new FileDataItem();
            item.setKey(flatKey);
            item.setPath(flatKey);
            item.setValue(value);

            String c = joinSummary(pendingSummaryComments);
            if (c != null) {
                item.setComment(c);
            }
            pendingSummaryComments.clear();

            result.put(flatKey, item);
            structure.add(LineEntry.data(currentSection, flatKey));
        }

        if (currentSection != null && currentSectionCount == 0) {
            String phKey = currentSection + "[0]";
            if (!result.containsKey(phKey)) {
                FileDataItem ph = new FileDataItem();
                ph.setKey(phKey);
                ph.setPath(phKey);
                ph.setValue("");
                result.put(phKey, ph);
            }
        }

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        String ls = (originalLineSeparator == null || originalLineSeparator.isEmpty())
                ? System.lineSeparator()
                : originalLineSeparator;

        StringBuilder sb = new StringBuilder();

        if (structure.isEmpty()) {
            boolean first = true;
            for (FileDataItem item : data.values()) {
                if (item == null || item.getKey() == null) {
                    continue;
                }
                if (!first) {
                    sb.append(ls);
                }
                String value = item.getValue() == null ? "" : item.getValue().toString();
                sb.append(item.getKey()).append("=").append(value);
                first = false;
            }
            return sb.toString();
        }

        for (LineEntry entry : structure) {
            if (entry.type == LineType.EMPTY) {
                sb.append(ls);
                continue;
            }
            if (entry.type == LineType.COMMENT) {
                sb.append(entry.lineText == null ? "" : entry.lineText).append(ls);
                continue;
            }
            if (entry.type == LineType.SECTION) {
                sb.append("[").append(entry.sectionName).append("]").append(ls);
                continue;
            }
            if (entry.type == LineType.DATA) {
                FileDataItem item = data.get(entry.flatKey);
                if (item != null) {
                    sb.append(formatSectionDataLine(entry.flatKey, item)).append(ls);
                }
                continue;
            }
            if (entry.type == LineType.GLOBAL_DATA) {
                FileDataItem item = data.get(entry.flatKey);
                if (item != null) {
                    String value = item.getValue() == null ? "" : item.getValue().toString();
                    sb.append(entry.flatKey).append("=").append(value).append(ls);
                }
            }
        }

        if (!originalEndsWithNewline) {
            if (sb.length() >= ls.length() && sb.substring(sb.length() - ls.length()).equals(ls)) {
                sb.setLength(sb.length() - ls.length());
            } else if (sb.length() > 0) {
                char last = sb.charAt(sb.length() - 1);
                if (last == '\n' || last == '\r') {
                    sb.setLength(sb.length() - 1);
                }
            }
        }

        return sb.toString();
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation of INI files is not implemented yet.");
    }

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.length() >= 2 && trimmed.charAt(0) == '[' && trimmed.charAt(trimmed.length() - 1) == ']';
    }

    private static boolean isCommentLine(String trimmed) {
        return !trimmed.isEmpty() && (trimmed.charAt(0) == '#' || trimmed.charAt(0) == ';');
    }

    private static String detectLineSeparator(String data) {
        if (data == null || data.isEmpty()) {
            return System.lineSeparator();
        }
        if (data.contains("\r\n")) {
            return "\r\n";
        }
        if (data.indexOf('\n') >= 0) {
            return "\n";
        }
        if (data.indexOf('\r') >= 0) {
            return "\r";
        }
        return System.lineSeparator();
    }

    private static String toSummaryComment(String trimmedCommentLine) {
        if (trimmedCommentLine == null || trimmedCommentLine.isEmpty()) {
            return null;
        }
        char c0 = trimmedCommentLine.charAt(0);
        if (c0 != '#' && c0 != ';') {
            return null;
        }

        String s = trimmedCommentLine.substring(1);
        if (!s.isEmpty() && s.charAt(0) == ' ') {
            s = s.substring(1);
        }
        s = s.trim();
        if (s.isEmpty()) {
            return null;
        }

        if (s.contains("ansible_") || s.contains("ansible-")) {
            return null;
        }

        String first = firstToken(s);
        if (isLikelyHostname(first)) {
            return null;
        }

        return s;
    }

    private static String firstToken(String s) {
        int i = 0;
        while (i < s.length() && !Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return s.substring(0, i);
    }

    private static boolean isLikelyHostname(String token) {
        if (token == null || token.isEmpty()) {
            return false;
        }
        for (int i = 0; i < token.length(); i++) {
            char ch = token.charAt(i);
            if (!(Character.isLetterOrDigit(ch) || ch == '.' || ch == '-')) {
                return false;
            }
        }
        return token.indexOf('.') >= 0;
    }

    private static String joinSummary(List<String> pending) {
        if (pending == null || pending.isEmpty()) {
            return null;
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

    private static String formatSectionDataLine(String flatKey, FileDataItem item) {
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

    private enum LineType {
        EMPTY,
        COMMENT,
        SECTION,
        DATA,
        GLOBAL_DATA
    }

    private static final class LineEntry {
        final LineType type;
        final String sectionName;
        final String flatKey;
        final String lineText;

        private LineEntry(LineType type, String sectionName, String flatKey, String lineText) {
            this.type = type;
            this.sectionName = sectionName;
            this.flatKey = flatKey;
            this.lineText = lineText;
        }

        static LineEntry empty() {
            return new LineEntry(LineType.EMPTY, null, null, null);
        }

        static LineEntry comment(String raw) {
            return new LineEntry(LineType.COMMENT, null, null, raw);
        }

        static LineEntry section(String sectionName) {
            return new LineEntry(LineType.SECTION, sectionName, null, null);
        }

        static LineEntry data(String sectionName, String flatKey) {
            return new LineEntry(LineType.DATA, sectionName, flatKey, null);
        }

        static LineEntry globalData(String key) {
            return new LineEntry(LineType.GLOBAL_DATA, null, key, null);
        }
    }
}