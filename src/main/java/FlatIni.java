import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatIni implements FlatService {

    private final List<LineEntry> structure = new ArrayList<>();

    private boolean originalEndsWithNewline = false;

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        Map<String, FileDataItem> result = new LinkedHashMap<>();
        structure.clear();
        originalEndsWithNewline = false;

        if (data == null || data.isBlank()) {
            return result;
        }

        originalEndsWithNewline = data.endsWith("\n") || data.endsWith("\r");

        String[] lines = data.split("\\R", -1);

        String currentSection = null;
        Map<String, Integer> sectionLineIndex = new LinkedHashMap<>();

        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                structure.add(LineEntry.empty());
                continue;
            }

            if (trimmed.startsWith("#") || trimmed.startsWith(";")) {
                structure.add(LineEntry.comment(line));
                continue;
            }

            if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                currentSection = trimmed.substring(1, trimmed.length() - 1).trim();
                structure.add(LineEntry.section(currentSection));

                if (!sectionLineIndex.containsKey(currentSection)) {
                    sectionLineIndex.put(currentSection, 0);
                }
                continue;
            }

            String dataLine = trimmed;

            if (currentSection == null) {
                int eq = dataLine.indexOf('=');
                if (eq < 0) {
                    continue;
                }
                String key = dataLine.substring(0, eq).trim();
                String value = dataLine.substring(eq + 1).trim();

                FileDataItem item = new FileDataItem();
                item.setKey(key);
                item.setValue(value);
                result.put(key, item);

                structure.add(LineEntry.globalData(key));
                continue;
            }

            int idx = sectionLineIndex.getOrDefault(currentSection, 0);
            sectionLineIndex.put(currentSection, idx + 1);

            String flatKey;
            String value;

            boolean hasWhitespace = containsWhitespace(dataLine);
            boolean isChildrenSection = currentSection.contains(":children");

            if (isChildrenSection || !hasWhitespace) {
                flatKey = currentSection + "[" + idx + "]";
                value = dataLine;
            } else {
                int ws = firstWhitespaceIndex(dataLine);
                String host = dataLine.substring(0, ws);
                String rest = dataLine.substring(ws).trim();

                flatKey = currentSection + "[" + idx + "]." + host;
                value = rest;
            }

            FileDataItem item = new FileDataItem();
            item.setKey(flatKey);
            item.setValue(value);
            result.put(flatKey, item);

            structure.add(LineEntry.data(currentSection, flatKey));
        }

        for (Map.Entry<String, Integer> e : sectionLineIndex.entrySet()) {
            String section = e.getKey();
            int count = e.getValue();
            if (count == 0) {
                String flatKey = section + "[0]";
                if (!result.containsKey(flatKey)) {
                    FileDataItem item = new FileDataItem();
                    item.setKey(flatKey);
                    item.setValue("");
                    result.put(flatKey, item);
                }
            }
        }

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        String ls = System.lineSeparator();

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
                sb.append(item.getKey()).append(" = ").append(value);
                first = false;
            }
            return sb.toString();
        }

        for (LineEntry entry : structure) {
            switch (entry.type) {
                case EMPTY:
                    sb.append(ls);
                    break;

                case COMMENT:
                    if (entry.lineText != null) {
                        sb.append(entry.lineText).append(ls);
                    } else {
                        sb.append(ls);
                    }
                    break;

                case SECTION:
                    sb.append("[")
                            .append(entry.sectionName)
                            .append("]")
                            .append(ls);
                    break;

                case DATA: {
                    FileDataItem item = data.get(entry.flatKey);
                    if (item == null || item.getKey() == null) {
                        continue;
                    }
                    String line = formatDataLine(entry.flatKey, item);
                    sb.append(line).append(ls);
                    break;
                }

                case GLOBAL_DATA: {
                    FileDataItem item = data.get(entry.flatKey);
                    if (item == null || item.getKey() == null) {
                        continue;
                    }
                    String value = item.getValue() == null ? "" : item.getValue().toString();
                    sb.append(entry.flatKey).append("=").append(value).append(ls);
                    break;
                }

                default:
                    break;
            }
        }

        String result = sb.toString();
        if (!originalEndsWithNewline) {
            if (result.endsWith(ls)) {
                result = result.substring(0, result.length() - ls.length());
            } else if (result.endsWith("\n") || result.endsWith("\r")) {
                result = result.substring(0, result.length() - 1);
            }
        }

        return result;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation of INI files is not implemented yet.");
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
        String value = (item.getValue() == null) ? "" : item.getValue().toString();

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

        static LineEntry comment(String lineText) {
            return new LineEntry(LineType.COMMENT, null, null, lineText);
        }

        static LineEntry section(String sectionName) {
            return new LineEntry(LineType.SECTION, sectionName, null, null);
        }

        static LineEntry data(String sectionName, String flatKey) {
            return new LineEntry(LineType.DATA, sectionName, flatKey, null);
        }

        static LineEntry globalData(String flatKey) {
            return new LineEntry(LineType.GLOBAL_DATA, null, flatKey, null);
        }
    }
}
