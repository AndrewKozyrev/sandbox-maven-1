import org.apache.commons.lang3.NotImplementedException;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.comments.CommentLine;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.nodes.*;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

import java.io.StringReader;
import java.util.*;

public class FlatYaml implements FlatService {

    private final Yaml yamlWithComments;
    private final Yaml yamlDump;
    private final Yaml yamlForLineIndex;

    private final Map<String, CommentState> stateById = new HashMap<>();

    public FlatYaml() {
        LoaderOptions loaderOptionsComments = new LoaderOptions();
        loaderOptionsComments.setProcessComments(true);
        DumperOptions dummyDumpOptions = new DumperOptions();
        dummyDumpOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Representer dummyRep = new Representer(dummyDumpOptions);
        this.yamlWithComments = new Yaml(
                new SafeConstructor(loaderOptionsComments),
                dummyRep,
                dummyDumpOptions,
                loaderOptionsComments
        );

        DumperOptions dumpOptions = new DumperOptions();
        dumpOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        dumpOptions.setPrettyFlow(true);
        dumpOptions.setIndent(2);
        dumpOptions.setIndicatorIndent(2);
        dumpOptions.setIndentWithIndicator(true);
        ScalarSpecRepresenter rep = new ScalarSpecRepresenter(dumpOptions);
        this.yamlDump = new Yaml(new SafeConstructor(new LoaderOptions()), rep, dumpOptions);

        LoaderOptions loaderOptionsNoComments = new LoaderOptions();
        loaderOptionsNoComments.setProcessComments(false);
        DumperOptions opt2 = new DumperOptions();
        this.yamlForLineIndex = new Yaml(
                new SafeConstructor(loaderOptionsNoComments),
                new Representer(opt2),
                opt2,
                loaderOptionsNoComments
        );
    }

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        Map<String, FileDataItem> result = new LinkedHashMap<>();

        if (data == null || data.isBlank()) {
            return result;
        }

        Node root = yamlWithComments.compose(new StringReader(data));
        if (root == null) {
            return result;
        }

        String sourceId = UUID.randomUUID().toString();
        CommentState cs = new CommentState();
        cs.headerComment = extractBlockAndEndComments(root);

        visitNode(root, null, result, null, true, cs, sourceId);

        stateById.put(sourceId, cs);

        return result;
    }

    private void visitNode(Node node,
                           String currentPath,
                           Map<String, FileDataItem> out,
                           String inheritedComment,
                           boolean inheritOnlyOnce,
                           CommentState cs,
                           String sourceId) {

        if (node instanceof MappingNode) {
            MappingNode mappingNode = (MappingNode) node;
            String remainingInherited = inheritedComment;

            List<NodeTuple> tuples = mappingNode.getValue();
            for (int i = 0; i < tuples.size(); i++) {
                NodeTuple tuple = tuples.get(i);
                Node keyNode = tuple.getKeyNode();
                Node valueNode = tuple.getValueNode();

                if (!(keyNode instanceof ScalarNode)) {
                    continue;
                }
                ScalarNode keyScalar = (ScalarNode) keyNode;
                String key = keyScalar.getValue();

                String childPath = (currentPath == null || currentPath.isEmpty())
                        ? key
                        : currentPath + "." + key;

                String keyInline = extractInlineComments(keyNode);
                if (keyInline != null && !keyInline.trim().isEmpty()) {
                    cs.keyInlineComments.put(childPath, keyInline.trim());
                }

                String keyBlock = extractBlockAndEndComments(keyNode);
                if (keyBlock != null && !keyBlock.trim().isEmpty()) {
                    cs.keyBlockComments.put(childPath, keyBlock.trim());
                }

                String ownComments = extractComments(valueNode);
                String commentsForChild = mergeComments(remainingInherited, ownComments);

                if (inheritOnlyOnce && remainingInherited != null && !remainingInherited.trim().isEmpty()) {
                    remainingInherited = null;
                }

                if (valueNode instanceof ScalarNode) {
                    createItem(childPath, sourceId, (ScalarNode) valueNode, commentsForChild, out, cs);
                } else {
                    visitNode(valueNode, childPath, out, commentsForChild, true, cs, sourceId);
                }
            }
        } else if (node instanceof SequenceNode) {
            SequenceNode sequenceNode = (SequenceNode) node;
            List<Node> elements = sequenceNode.getValue();

            String remainingInherited = inheritedComment;

            for (int i = 0; i < elements.size(); i++) {
                Node child = elements.get(i);
                String childPath = (currentPath == null || currentPath.isEmpty())
                        ? Integer.toString(i)
                        : currentPath + "[" + i + "]";

                String ownComments = extractComments(child);
                String commentsForChild = mergeComments(remainingInherited, ownComments);

                if (inheritOnlyOnce && remainingInherited != null && !remainingInherited.trim().isEmpty()) {
                    remainingInherited = null;
                }

                if (child instanceof ScalarNode) {
                    createItem(childPath, sourceId, (ScalarNode) child, commentsForChild, out, cs);
                } else {
                    visitNode(child, childPath, out, commentsForChild, true, cs, sourceId);
                }
            }
        } else if (node instanceof ScalarNode) {
            ScalarNode scalarNode = (ScalarNode) node;
            String ownComments = extractComments(node);
            String comments = mergeComments(inheritedComment, ownComments);
            String path = (currentPath == null) ? "" : currentPath;
            createItem(path, sourceId, scalarNode, comments, out, cs);
        }
    }

    private void createItem(String path,
                            String sourceId,
                            ScalarNode scalarNode,
                            String comments,
                            Map<String, FileDataItem> out,
                            CommentState cs) {

        FileDataItem item = new FileDataItem();
        item.setKey(path);
        item.setPath(path);
        item.setFilename(sourceId);
        item.setComment(comments);
        item.setValue(constructScalarValue(scalarNode));

        out.put(path, item);

        if (path != null && !path.isEmpty()) {
            Tag tag = scalarNode.getTag();
            DumperOptions.ScalarStyle style = scalarNode.getScalarStyle();
            cs.scalarStyles.put(path, new ScalarStyleInfo(tag, style));
        }
    }

    private String constructScalarValue(ScalarNode scalar) {
        String value = scalar.getValue();
        DumperOptions.ScalarStyle style = scalar.getScalarStyle();

        if (style == DumperOptions.ScalarStyle.DOUBLE_QUOTED) {
            return "\"" + value + "\"";
        } else if (style == DumperOptions.ScalarStyle.SINGLE_QUOTED) {
            return "'" + value + "'";
        } else {
            return value;
        }
    }

    private String extractComments(Node node) {
        if (node == null) {
            return null;
        }

        List<String> lines = new ArrayList<>();

        List<CommentLine> block = node.getBlockComments();
        if (block != null) {
            for (CommentLine cl : block) {
                if (cl != null && cl.getValue() != null) {
                    lines.add(cl.getValue().trim());
                }
            }
        }

        List<CommentLine> inline = node.getInLineComments();
        if (inline != null) {
            for (CommentLine cl : inline) {
                if (cl != null && cl.getValue() != null) {
                    lines.add(cl.getValue().trim());
                }
            }
        }

        List<CommentLine> end = node.getEndComments();
        if (end != null) {
            for (CommentLine cl : end) {
                if (cl != null && cl.getValue() != null) {
                    lines.add(cl.getValue().trim());
                }
            }
        }

        if (lines.isEmpty()) {
            return null;
        }

        return joinLines(lines);
    }

    private String extractInlineComments(Node node) {
        if (node == null) {
            return null;
        }
        List<CommentLine> inline = node.getInLineComments();
        if (inline == null || inline.isEmpty()) {
            return null;
        }
        List<String> lines = new ArrayList<>();
        for (CommentLine cl : inline) {
            if (cl != null && cl.getValue() != null) {
                lines.add(cl.getValue().trim());
            }
        }
        if (lines.isEmpty()) {
            return null;
        }
        return joinLines(lines);
    }

    private String extractBlockAndEndComments(Node node) {
        if (node == null) {
            return null;
        }
        List<String> lines = new ArrayList<>();

        List<CommentLine> block = node.getBlockComments();
        if (block != null) {
            for (CommentLine cl : block) {
                if (cl != null && cl.getValue() != null) {
                    lines.add(cl.getValue().trim());
                }
            }
        }

        List<CommentLine> end = node.getEndComments();
        if (end != null) {
            for (CommentLine cl : end) {
                if (cl != null && cl.getValue() != null) {
                    lines.add(cl.getValue().trim());
                }
            }
        }

        if (lines.isEmpty()) {
            return null;
        }
        return joinLines(lines);
    }

    private String joinLines(List<String> lines) {
        StringBuilder sb = new StringBuilder();
        String ls = System.lineSeparator();
        for (int i = 0; i < lines.size(); i++) {
            if (i > 0) {
                sb.append(ls);
            }
            sb.append(lines.get(i));
        }
        return sb.toString();
    }

    private String mergeComments(String parent, String own) {
        boolean parentEmpty = (parent == null || parent.isBlank());
        boolean ownEmpty = (own == null || own.isBlank());

        if (parentEmpty && ownEmpty) {
            return null;
        }
        if (parentEmpty) {
            return own.trim();
        }
        if (ownEmpty) {
            return parent.trim();
        }
        return parent.trim() + System.lineSeparator() + own.trim();
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        String sourceId = null;
        for (FileDataItem item : data.values()) {
            if (item != null && item.getFilename() != null && !item.getFilename().isBlank()) {
                sourceId = item.getFilename();
                break;
            }
        }

        CommentState cs = (sourceId == null) ? null : stateById.get(sourceId);
        String headerComment = cs == null ? null : cs.headerComment;
        Map<String, String> keyBlockComments =
                cs == null ? Collections.emptyMap() : cs.keyBlockComments;
        Map<String, String> keyInlineComments =
                cs == null ? Collections.emptyMap() : cs.keyInlineComments;

        Map<Object, Object> root = new LinkedHashMap<>();
        for (Map.Entry<String, FileDataItem> entry : data.entrySet()) {
            String path = entry.getKey();
            FileDataItem item = entry.getValue();
            if (path == null || path.isBlank() || item == null) {
                continue;
            }
            insertValue(root, path, item, cs);
        }

        String yamlText = yamlDump.dump(root);

        LineIndexes lineIndexes = buildPathLineIndex(yamlText);
        Map<String, Integer> pathToValueLine = lineIndexes.valueLines;
        Map<String, Integer> pathToKeyLine = lineIndexes.keyLines;

        Map<Integer, List<String>> commentsByLine = new HashMap<>();
        Map<Integer, String> inlineCommentsByLine = new HashMap<>();

        for (Map.Entry<String, FileDataItem> entry : data.entrySet()) {
            String path = entry.getKey();
            FileDataItem item = entry.getValue();
            if (item == null) {
                continue;
            }

            String comment = item.getComment();
            if (comment == null || comment.trim().isEmpty()) {
                continue;
            }

            Integer lineIndex = pathToValueLine.get(path);
            if (lineIndex == null || lineIndex < 0) {
                continue;
            }

            String[] commentLines = comment.split("\\R");
            List<String> nonBlank = new ArrayList<String>();
            for (String cLine : commentLines) {
                if (cLine != null && !cLine.trim().isEmpty()) {
                    nonBlank.add(cLine.trim());
                }
            }
            if (nonBlank.isEmpty()) {
                continue;
            }

            if (nonBlank.size() == 1) {
                inlineCommentsByLine.put(lineIndex, "# " + nonBlank.get(0));
            } else {
                List<String> list = commentsByLine.computeIfAbsent(lineIndex, k -> new ArrayList<>());
                for (String s : nonBlank) {
                    list.add("# " + s);
                }
            }
        }

        for (Map.Entry<String, String> entry : keyBlockComments.entrySet()) {
            String path = entry.getKey();
            String comment = entry.getValue();
            if (comment == null || comment.trim().isEmpty()) {
                continue;
            }

            Integer lineIndex = pathToKeyLine.get(path);
            if (lineIndex == null || lineIndex < 0) {
                continue;
            }

            List<String> list = commentsByLine.computeIfAbsent(lineIndex, k -> new ArrayList<>());

            String[] commentLines = comment.split("\\R");
            for (String cLine : commentLines) {
                if (cLine == null || cLine.trim().isEmpty()) {
                    continue;
                }
                list.add("# " + cLine.trim());
            }
        }

        for (Map.Entry<String, String> entry : keyInlineComments.entrySet()) {
            String path = entry.getKey();
            String comment = entry.getValue();
            if (comment == null || comment.trim().isEmpty()) {
                continue;
            }

            Integer lineIndex = pathToKeyLine.get(path);
            if (lineIndex == null || lineIndex < 0) {
                continue;
            }

            String[] commentLines = comment.split("\\R");
            String first = null;
            for (int i = 0; i < commentLines.length && first == null; i++) {
                String cLine = commentLines[i];
                if (cLine != null && !cLine.trim().isEmpty()) {
                    first = cLine.trim();
                }
            }
            if (first == null) {
                continue;
            }

            inlineCommentsByLine.put(lineIndex, "# " + first);
        }

        if (headerComment != null && !headerComment.trim().isEmpty()) {
            int minKeyLine = Integer.MAX_VALUE;
            for (Integer v : pathToKeyLine.values()) {
                if (v != null && v < minKeyLine) {
                    minKeyLine = v;
                }
            }
            if (minKeyLine == Integer.MAX_VALUE) {
                minKeyLine = 0;
            }

            List<String> list = commentsByLine.computeIfAbsent(minKeyLine, k -> new ArrayList<>());

            String[] headerLines = headerComment.split("\\R");
            for (String hLine : headerLines) {
                if (hLine == null || hLine.trim().isEmpty()) {
                    continue;
                }
                list.add("# " + hLine.trim());
            }
        }

        String[] lines = yamlText.split("\\R", -1);
        String ls = System.lineSeparator();
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];

            int pos = 0;
            while (pos < line.length() && Character.isWhitespace(line.charAt(pos))) {
                pos++;
            }
            String indent = line.substring(0, pos);

            List<String> cl = commentsByLine.get(i);
            if (cl != null) {
                for (String c : cl) {
                    sb.append(indent).append(c).append(ls);
                }
            }

            String inline = inlineCommentsByLine.get(i);
            if (inline != null) {
                sb.append(line).append(" ").append(inline);
            } else {
                sb.append(line);
            }

            if (i < lines.length - 1) {
                sb.append(ls);
            }
        }

        return sb.toString();
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation is not implemented");
    }

    @SuppressWarnings("unchecked")
    private void insertValue(Map<Object, Object> root,
                             String path,
                             FileDataItem item,
                             CommentState cs) {
        List<PathToken> tokens = parsePath(path);
        if (tokens.isEmpty()) {
            return;
        }

        Object container = root;
        Object value = wrapValue(path, (String) item.getValue(), cs);

        for (int i = 0; i < tokens.size(); i++) {
            PathToken token = tokens.get(i);
            boolean last = (i == tokens.size() - 1);

            if (!token.isIndex()) {
                String key = token.key;
                if (!(container instanceof Map)) {
                    throw new IllegalStateException("Expected Map for key '" + key + "' in path '" + path + "'");
                }
                Map<Object, Object> map = (Map<Object, Object>) container;

                if (last) {
                    map.put(key, value);
                } else {
                    Object child = map.get(key);
                    if (child == null) {
                        PathToken next = tokens.get(i + 1);
                        Object newContainer = next.isIndex()
                                ? new ArrayList<>()
                                : new LinkedHashMap<>();
                        map.put(key, newContainer);
                        child = newContainer;
                    }
                    container = child;
                }
            } else {
                int index = token.index;
                if (!(container instanceof List)) {
                    throw new IllegalStateException("Expected List for index [" + index + "] in path '" + path + "'");
                }
                List<Object> list = (List<Object>) container;
                while (list.size() <= index) {
                    list.add(null);
                }

                if (last) {
                    list.set(index, value);
                } else {
                    Object child = list.get(index);
                    if (child == null) {
                        PathToken next = tokens.get(i + 1);
                        Object newContainer = next.isIndex()
                                ? new ArrayList<>()
                                : new LinkedHashMap<>();
                        list.set(index, newContainer);
                        child = newContainer;
                    }
                    container = child;
                }
            }
        }
    }

    private Object wrapValue(String path, String v, CommentState cs) {
        if (cs == null) {
            return v;
        }
        ScalarStyleInfo info = cs.scalarStyles.get(path);
        if (info == null) {
            return v;
        }

        String raw = v;
        if (v != null && v.length() >= 2) {
            if (info.style == DumperOptions.ScalarStyle.DOUBLE_QUOTED
                    && v.startsWith("\"") && v.endsWith("\"")) {
                raw = v.substring(1, v.length() - 1);
            } else if (info.style == DumperOptions.ScalarStyle.SINGLE_QUOTED
                    && v.startsWith("'") && v.endsWith("'")) {
                raw = v.substring(1, v.length() - 1);
            }
        }

        return new ScalarSpec(raw, info.tag, info.style);
    }

    private List<PathToken> parsePath(String path) {
        List<PathToken> tokens = new ArrayList<>();
        if (path == null || path.isBlank()) {
            return tokens;
        }

        int i = 0;
        int len = path.length();

        while (i < len) {
            int start = i;
            while (i < len && path.charAt(i) != '.' && path.charAt(i) != '[') {
                i++;
            }
            if (i > start) {
                String key = path.substring(start, i);
                tokens.add(PathToken.key(key));
            }
            while (i < len && path.charAt(i) == '[') {
                int close = path.indexOf(']', i);
                if (close == -1) {
                    throw new IllegalArgumentException("Unmatched '[' in path: " + path);
                }
                String idxStr = path.substring(i + 1, close);
                int idx;
                try {
                    idx = Integer.parseInt(idxStr);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Non-numeric index '" + idxStr + "' in path: " + path, e);
                }
                tokens.add(PathToken.index(idx));
                i = close + 1;
            }
            if (i < len && path.charAt(i) == '.') {
                i++;
            }
        }

        return tokens;
    }

    private LineIndexes buildPathLineIndex(String yamlText) {
        Map<String, Integer> valueLines = new HashMap<>();
        Map<String, Integer> keyLines = new HashMap<>();

        Node root = yamlForLineIndex.compose(new StringReader(yamlText));
        if (root != null) {
            visitNodeForLines(root, null, valueLines, keyLines);
        }

        return new LineIndexes(valueLines, keyLines);
    }

    private void visitNodeForLines(Node node,
                                   String currentPath,
                                   Map<String, Integer> valueLines,
                                   Map<String, Integer> keyLines) {

        if (node instanceof MappingNode) {
            MappingNode mappingNode = (MappingNode) node;
            List<NodeTuple> tuples = mappingNode.getValue();
            for (NodeTuple tuple : tuples) {
                Node keyNode = tuple.getKeyNode();
                Node valueNode = tuple.getValueNode();
                if (!(keyNode instanceof ScalarNode)) {
                    continue;
                }
                ScalarNode keyScalar = (ScalarNode) keyNode;
                String key = keyScalar.getValue();

                String childPath = (currentPath == null || currentPath.isEmpty())
                        ? key
                        : currentPath + "." + key;

                Mark keyMark = keyScalar.getStartMark();
                if (keyMark != null) {
                    keyLines.put(childPath, keyMark.getLine());
                }

                if (valueNode instanceof ScalarNode) {
                    ScalarNode scalar = (ScalarNode) valueNode;
                    Mark m = scalar.getStartMark();
                    if (m != null) {
                        valueLines.put(childPath, m.getLine());
                    }
                } else {
                    visitNodeForLines(valueNode, childPath, valueLines, keyLines);
                }
            }
        } else if (node instanceof SequenceNode) {
            SequenceNode sequenceNode = (SequenceNode) node;
            List<Node> elements = sequenceNode.getValue();
            for (int i = 0; i < elements.size(); i++) {
                Node child = elements.get(i);
                String childPath = (currentPath == null || currentPath.isEmpty())
                        ? Integer.toString(i)
                        : currentPath + "[" + i + "]";

                if (child instanceof ScalarNode) {
                    ScalarNode scalar = (ScalarNode) child;
                    Mark m = scalar.getStartMark();
                    if (m != null) {
                        valueLines.put(childPath, m.getLine());
                    }
                } else {
                    visitNodeForLines(child, childPath, valueLines, keyLines);
                }
            }
        } else if (node instanceof ScalarNode) {
            ScalarNode scalar = (ScalarNode) node;
            String path = (currentPath == null) ? "" : currentPath;
            Mark m = scalar.getStartMark();
            if (m != null && !path.isEmpty()) {
                valueLines.put(path, m.getLine());
            }
        }
    }

    private static final class PathToken {
        final String key;
        final Integer index;

        private PathToken(String key, Integer index) {
            this.key = key;
            this.index = index;
        }

        static PathToken key(String key) {
            return new PathToken(key, null);
        }

        static PathToken index(int idx) {
            return new PathToken(null, idx);
        }

        boolean isIndex() {
            return index != null;
        }
    }

    private static final class LineIndexes {
        final Map<String, Integer> valueLines;
        final Map<String, Integer> keyLines;

        LineIndexes(Map<String, Integer> valueLines, Map<String, Integer> keyLines) {
            this.valueLines = valueLines;
            this.keyLines = keyLines;
        }
    }

    private static final class ScalarStyleInfo {
        final Tag tag;
        final DumperOptions.ScalarStyle style;

        ScalarStyleInfo(Tag tag, DumperOptions.ScalarStyle style) {
            this.tag = tag;
            this.style = style;
        }
    }

    private static final class CommentState {
        String headerComment;
        final Map<String, String> keyBlockComments = new HashMap<>();
        final Map<String, String> keyInlineComments = new HashMap<>();
        final Map<String, ScalarStyleInfo> scalarStyles = new HashMap<>();
    }

    private static final class ScalarSpec {
        final String value;
        final Tag tag;
        final DumperOptions.ScalarStyle style;

        ScalarSpec(String value, Tag tag, DumperOptions.ScalarStyle style) {
            this.value = value;
            this.tag = tag;
            this.style = style;
        }
    }

    private static class ScalarSpecRepresenter extends Representer {

        ScalarSpecRepresenter(DumperOptions options) {
            super(options);
            this.representers.put(ScalarSpec.class, new RepresentScalarSpec());
        }

        private class RepresentScalarSpec implements Represent {
            @Override
            public Node representData(Object data) {
                ScalarSpec s = (ScalarSpec) data;
                Tag tag = (s.tag != null) ? s.tag : Tag.STR;
                DumperOptions.ScalarStyle style =
                        (s.style != null) ? s.style : DumperOptions.ScalarStyle.PLAIN;
                String value = (s.value == null) ? "" : s.value;
                return representScalar(tag, value, style);
            }
        }
    }
}