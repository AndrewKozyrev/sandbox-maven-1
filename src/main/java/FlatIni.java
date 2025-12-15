import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatIni implements FlatService {

    private final List<LineEntry> structure = new ArrayList<>();

    private boolean originalEndsWithNewline = false;
    private String originalLineSeparator = System.lineSeparator();

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        Map<String, FileDataItem> result = new LinkedHashMap<>();
        structure.clear();
        originalEndsWithNewline = false;
        originalLineSeparator = System.lineSeparator();

        if (data == null || data.isBlank()) {
            return result;
        }

        originalEndsWithNewline = data.endsWith("\n") || data.endsWith("\r");
        originalLineSeparator = detectLineSeparator(data);

        String[] lines = data.split("\\R");

        String currentSection = null;
        Map<String, Integer> sectionLineIndex = new LinkedHashMap<>();
        Map<String, Boolean> hasPlaceholder = new HashMap<>();

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

            if (isSectionHeader(trimmed)) {
                currentSection = trimmed.substring(1, trimmed.length() - 1).trim();
                structure.add(LineEntry.section(currentSection));

                sectionLineIndex.putIfAbsent(currentSection, 0);

                String placeholderKey = currentSection + "[0]";
                if (!result.containsKey(placeholderKey)) {
                    FileDataItem item = new FileDataItem();
                    item.setKey(placeholderKey);
                    item.setPath(placeholderKey);
                    item.setValue("");
                    result.put(placeholderKey, item);
                    hasPlaceholder.put(currentSection, Boolean.TRUE);
                }
                continue;
            }

            if (currentSection == null) {
                int eq = trimmed.indexOf('=');
                String key;
                String value;
                if (eq >= 0) {
                    key = trimmed.substring(0, eq).trim();
                    value = trimmed.substring(eq + 1).trim();
                } else {
                    key = trimmed.trim();
                    value = "";
                }
                if (!key.isEmpty()) {
                    FileDataItem item = new FileDataItem();
                    item.setKey(key);
                    item.setPath(key);
                    item.setValue(value);
                    result.put(key, item);
                    structure.add(LineEntry.global(key));
                }
                continue;
            }

            if (Boolean.TRUE.equals(hasPlaceholder.get(currentSection))) {
                result.remove(currentSection + "[0]");
                hasPlaceholder.put(currentSection, Boolean.FALSE);
            }

            int idx = sectionLineIndex.getOrDefault(currentSection, 0);
            sectionLineIndex.put(currentSection, idx + 1);

            boolean hasWhitespace = containsWhitespace(trimmed);
            boolean isChildrenSection = currentSection.contains(":children");

            String flatKey;
            String value;

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
            result.put(flatKey, item);

            structure.add(LineEntry.data(flatKey));
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
                sb.append(item.getKey()).append(" = ").append(value);
                first = false;
            }
            return sb.toString();
        }

        for (LineEntry entry : structure) {
            if (entry.type == LineType.EMPTY) {
                sb.append(ls);
            } else if (entry.type == LineType.COMMENT) {
                sb.append(entry.text == null ? "" : entry.text).append(ls);
            } else if (entry.type == LineType.SECTION) {
                sb.append("[").append(entry.text).append("]").append(ls);
            } else if (entry.type == LineType.DATA) {
                FileDataItem item = data.get(entry.text);
                if (item == null || item.getKey() == null) {
                    continue;
                }
                sb.append(formatSectionDataLine(entry.text, item)).append(ls);
            } else if (entry.type == LineType.GLOBAL) {
                FileDataItem item = data.get(entry.text);
                if (item == null || item.getKey() == null) {
                    continue;
                }
                sb.append(formatGlobalLine(item)).append(ls);
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

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.length() >= 2 && trimmed.charAt(0) == '[' && trimmed.charAt(trimmed.length() - 1) == ']';
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

    private static String formatGlobalLine(FileDataItem item) {
        String key = item.getKey();
        String value = item.getValue() == null ? "" : item.getValue().toString();
        return key + "=" + value;
    }

    private static String detectLineSeparator(String s) {
        if (s.contains("\r\n")) {
            return "\r\n";
        }
        if (s.indexOf('\n') >= 0) {
            return "\n";
        }
        if (s.indexOf('\r') >= 0) {
            return "\r";
        }
        return System.lineSeparator();
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

        static LineEntry comment(String line) {
            return new LineEntry(LineType.COMMENT, line);
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
}