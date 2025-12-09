import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatConfYaml implements FlatService {

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

        String[] lines = data.split("\\R");
        String currentKey = null;
        StringBuilder currentValue = new StringBuilder();
        String currentComment = null;
        StringBuilder pendingComment = new StringBuilder();

        for (String line : lines) {
            if (line == null) {
                continue;
            }

            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                structure.add(LineEntry.empty());
                continue;
            }

            if (trimmed.startsWith("#")) {
                structure.add(LineEntry.comment(line));

                String text = trimmed.substring(1).trim();
                if (!text.isEmpty()) {
                    if (pendingComment.length() > 0) {
                        pendingComment.append(System.lineSeparator());
                    }
                    pendingComment.append(text);
                }
                continue;
            }

            boolean startsWithWhitespace = Character.isWhitespace(line.charAt(0));
            int colonIdx = line.indexOf(':');

            boolean isTopLevelKey =
                    !startsWithWhitespace &&
                            colonIdx > 0 &&
                            !trimmed.startsWith("- ");

            if (isTopLevelKey) {
                if (currentKey != null) {
                    FileDataItem item = new FileDataItem();
                    item.setKey(currentKey);
                    item.setValue(currentValue.toString());
                    if (currentComment != null && !currentComment.isBlank()) {
                        item.setComment(currentComment);
                    }
                    result.put(currentKey, item);
                }

                String key = line.substring(0, colonIdx).trim();
                String rawValuePart = line.substring(colonIdx + 1);
                String valuePart;
                String inlineComment = null;
                int commentIdx = findInlineCommentIndex(rawValuePart);
                if (commentIdx >= 0) {
                    valuePart = rawValuePart.substring(0, commentIdx).trim();
                    inlineComment = rawValuePart.substring(commentIdx + 1).trim();
                } else {
                    valuePart = rawValuePart.trim();
                }

                structure.add(LineEntry.key(key));

                currentKey = key;
                currentValue.setLength(0);
                currentValue.append(valuePart);

                String blockComments = pendingComment.length() == 0 ? null : pendingComment.toString();
                currentComment = mergeComments(blockComments, inlineComment);
                pendingComment.setLength(0);
            } else {
                if (currentKey != null) {
                    if (currentValue.length() > 0) {
                        currentValue.append(System.lineSeparator());
                    }
                    currentValue.append(line);
                }
            }
        }

        if (currentKey != null) {
            FileDataItem item = new FileDataItem();
            item.setKey(currentKey);
            item.setValue(currentValue.toString());
            if (currentComment != null && !currentComment.isBlank()) {
                item.setComment(currentComment);
            }
            result.put(currentKey, item);
        }

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        if (structure.isEmpty()) {
            for (FileDataItem item : data.values()) {
                if (item == null || item.getKey() == null) {
                    continue;
                }
                String comment = item.getComment();
                if (comment != null && !comment.isBlank()) {
                    String[] lines = comment.split("\\R");
                    for (String line : lines) {
                        if (line != null && !line.isBlank()) {
                            sb.append("# ")
                                    .append(line.trim())
                                    .append(System.lineSeparator());
                        }
                    }
                }
                sb.append(item.getKey()).append(": ");
                Object value = item.getValue();
                if (value != null) {
                    sb.append(value);
                }
                sb.append(System.lineSeparator());
            }

            String result = sb.toString();
            if (!originalEndsWithNewline) {
                String ls = System.lineSeparator();
                if (result.endsWith(ls)) {
                    result = result.substring(0, result.length() - ls.length());
                } else if (result.endsWith("\n") || result.endsWith("\r")) {
                    result = result.substring(0, result.length() - 1);
                }
            }
            return result;
        }

        for (LineEntry entry : structure) {
            switch (entry.type) {
                case EMPTY:
                    sb.append(System.lineSeparator());
                    break;
                case COMMENT:
                    if (entry.text != null) {
                        sb.append(entry.text).append(System.lineSeparator());
                    } else {
                        sb.append(System.lineSeparator());
                    }
                    break;
                case KEY:
                    String key = entry.text;
                    if (key == null) {
                        continue;
                    }
                    FileDataItem item = data.get(key);
                    if (item == null || item.getKey() == null) {
                        continue;
                    }
                    sb.append(item.getKey()).append(": ");
                    Object value = item.getValue();
                    if (value != null) {
                        sb.append(value);
                    }
                    sb.append(System.lineSeparator());
                    break;
            }
        }

        String result = sb.toString();
        if (!originalEndsWithNewline) {
            String ls = System.lineSeparator();
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
        throw new UnsupportedOperationException("Validation for conf YAML is not implemented.");
    }

    private static int findInlineCommentIndex(String text) {
        if (text == null) {
            return -1;
        }
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' && !inSingle) {
                inDouble = !inDouble;
            } else if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
            } else if (c == '#' && !inSingle && !inDouble) {
                return i;
            }
        }
        return -1;
    }

    private static String mergeComments(String block, String inline) {
        boolean blockEmpty = (block == null || block.isBlank());
        boolean inlineEmpty = (inline == null || inline.isBlank());

        if (blockEmpty && inlineEmpty) {
            return null;
        }
        if (blockEmpty) {
            return inline.trim();
        }
        if (inlineEmpty) {
            return block.trim();
        }
        return block.trim() + System.lineSeparator() + inline.trim();
    }

    private enum LineType {
        EMPTY,
        COMMENT,
        KEY
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

        static LineEntry key(String key) {
            return new LineEntry(LineType.KEY, key);
        }
    }
}
