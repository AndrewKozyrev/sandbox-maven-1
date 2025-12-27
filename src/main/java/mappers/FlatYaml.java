package mappers;

import org.apache.commons.lang3.NotImplementedException;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatYaml implements FlatService {

    private final Yaml yaml;

    public FlatYaml() {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(false);
        options.setIndent(2);
        options.setIndicatorIndent(2);
        options.setIndentWithIndicator(true);

        Representer representer = new ScalarSpecRepresenter(options);
        this.yaml = new Yaml(representer, options);
    }

    private static final class ScalarSpec {
        final String value;
        final DumperOptions.ScalarStyle style;
        final Tag tag;

        ScalarSpec(String value, DumperOptions.ScalarStyle style, Tag tag) {
            this.value = value;
            this.style = style;
            this.tag = tag;
        }
    }

    private static final class ScalarSpecRepresenter extends Representer {
        ScalarSpecRepresenter(DumperOptions options) {
            super(options);
            this.representers.put(ScalarSpec.class, new RepresentScalarSpec());
        }

        private class RepresentScalarSpec implements Represent {
            @Override
            public Node representData(Object data) {
                ScalarSpec s = (ScalarSpec) data;
                String v = (s.value == null) ? "" : s.value;
                DumperOptions.ScalarStyle st =
                        (s.style == null) ? DumperOptions.ScalarStyle.PLAIN : s.style;
                Tag t = (s.tag == null) ? Tag.STR : s.tag;
                return representScalar(t, v, st);
            }
        }
    }

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        Map<String, FileDataItem> flatData = new LinkedHashMap<>();

        if (data == null || data.isBlank()) {
            return flatData;
        }

        String preprocessed = preprocessExclamationScalars(data);

        Node rootNode = yaml.compose(new StringReader(preprocessed));
        if (rootNode == null) {
            return flatData;
        }

        Map<String, Object> flatDataRaw = flatten(rootNode, "");
        for (Map.Entry<String, Object> entry : flatDataRaw.entrySet()) {
            String path = entry.getKey();
            Object raw = entry.getValue();
            String valueStr = (raw == null) ? null : raw.toString();

            FileDataItem f = new FileDataItem();
            f.setKey(path);
            f.setPath(path);
            f.setValue(valueStr);
            flatData.put(path, f);
        }

        return flatData;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        Map<String, String> specials = new LinkedHashMap<>();
        Map<String, Object> inputMap = new LinkedHashMap<>();
        for (FileDataItem item : data.values()) {
            if (item != null) {
                String key = item.getKey();
                if (key != null) {
                    Object raw = item.getValue();
                    String str = (raw == null) ? "" : raw.toString();
                    ScalarSpec spec = parseScalarSpec(str, specials);
                    inputMap.put(key, spec);
                }
            }
        }

        Map<String, Object> root = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : inputMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            String normalizedKey = key.replaceAll("\\[(\\d+)]", ".$1");
            String[] parts = normalizedKey.split("\\.");

            putPath(root, parts, 0, value);
        }

        String dumped = yaml.dump(root);

        for (Map.Entry<String, String> e : specials.entrySet()) {
            String placeholder = e.getKey();
            String original = e.getValue();
            dumped = dumped.replace(placeholder, original);
        }

        String ls = System.lineSeparator();
        if (!"\n".equals(ls)) {
            dumped = dumped.replace("\n", ls);
        }

        return dumped;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Р’Р°Р»РёРґР°С†РёСЏ YAML С„Р°Р№Р»РѕРІ РЅРµ СЂРµР°Р»РёР·РѕРІР°РЅР°.");
    }

    private Map<String, Object> flatten(Node node, String parentKey) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (node instanceof MappingNode) {
            processMappingNode(result, (MappingNode) node, parentKey);
        } else if (node instanceof SequenceNode) {
            processSequenceNode(result, (SequenceNode) node, parentKey);
        } else if (node instanceof ScalarNode) {
            addScalarNode(result, (ScalarNode) node, parentKey);
        }
        return result;
    }

    private void processMappingNode(Map<String, Object> result,
                                    MappingNode mappingNode,
                                    String parentKey) {
        List<NodeTuple> tuples = mappingNode.getValue();
        for (NodeTuple tuple : tuples) {
            Node keyNode = tuple.getKeyNode();
            Node valueNode = tuple.getValueNode();

            if (!(keyNode instanceof ScalarNode)) {
                continue;
            }

            String key = ((ScalarNode) keyNode).getValue();
            String currentKey = parentKey.isEmpty()
                    ? key
                    : parentKey + "." + key;

            result.putAll(flatten(valueNode, currentKey));
        }
    }

    private void processSequenceNode(Map<String, Object> result,
                                     SequenceNode sequenceNode,
                                     String parentKey) {
        List<Node> nodes = sequenceNode.getValue();
        for (int i = 0; i < nodes.size(); i++) {
            Node item = nodes.get(i);
            String currentKey = parentKey + "[" + i + "]";
            if (item instanceof ScalarNode) {
                addScalarNode(result, (ScalarNode) item, currentKey);
            } else {
                result.putAll(flatten(item, currentKey));
            }
        }
    }

    private void addScalarNode(Map<String, Object> result,
                               ScalarNode scalarNode,
                               String parentKey) {
        DumperOptions.ScalarStyle style = scalarNode.getScalarStyle();
        String text = scalarNode.getValue();
        String value;
        if (style == DumperOptions.ScalarStyle.DOUBLE_QUOTED) {
            String normalized = text.replace("\\\"", "\"");
            if (normalized.length() >= 4
                    && normalized.charAt(0) == '!'
                    && normalized.charAt(1) == '"'
                    && normalized.charAt(normalized.length() - 1) == '"') {
                value = normalized;
            } else {
                value = "\"" + normalized + "\"";
            }
        } else if (style == DumperOptions.ScalarStyle.SINGLE_QUOTED) {
            value = "'" + text + "'";
        } else {
            value = text;
        }
        result.put(parentKey, value);
    }

    private ScalarSpec parseScalarSpec(String bare, Map<String, String> specials) {
        if (bare == null) {
            return new ScalarSpec("", DumperOptions.ScalarStyle.PLAIN, Tag.STR);
        }

        if (bare.length() >= 2 && bare.startsWith("\"") && bare.endsWith("\"")) {
            String raw = bare.substring(1, bare.length() - 1);
            return new ScalarSpec(raw, DumperOptions.ScalarStyle.DOUBLE_QUOTED, Tag.STR);
        }

        if (bare.length() >= 2 && bare.startsWith("'") && bare.endsWith("'")) {
            String raw = bare.substring(1, bare.length() - 1);
            return new ScalarSpec(raw, DumperOptions.ScalarStyle.SINGLE_QUOTED, Tag.STR);
        }

        if (bare.length() >= 4
                && bare.charAt(0) == '!'
                && bare.charAt(1) == '"'
                && bare.charAt(bare.length() - 1) == '"') {
            String placeholder = "EXCL_" + specials.size();
            specials.put(placeholder, bare);
            return new ScalarSpec(placeholder, DumperOptions.ScalarStyle.PLAIN, Tag.STR);
        }

        Tag tag;
        if (isIntegerLiteral(bare)) {
            tag = Tag.INT;
        } else {
            tag = Tag.STR;
        }
        return new ScalarSpec(bare, DumperOptions.ScalarStyle.PLAIN, tag);
    }

    private boolean isIntegerLiteral(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        int start = 0;
        char c0 = s.charAt(0);
        if (c0 == '-' || c0 == '+') {
            if (s.length() == 1) {
                return false;
            }
            start = 1;
        }
        for (int i = start; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private void putPath(Object container, String[] parts, int index, Object value) {
        Object current = container;
        for (int i = index; i < parts.length; i++) {
            String part = parts[i];
            boolean last = (i == parts.length - 1);
            String nextPart = last ? null : parts[i + 1];

            if (isInteger(part)) {
                int arrayIndex = Integer.parseInt(part);
                current = putToList((List<Object>) current, arrayIndex, last, nextPart, value);
            } else {
                current = putToMap((Map<String, Object>) current, part, last, nextPart, value);
            }
        }
    }

    private Object putToList(List<Object> list, int arrayIndex, boolean last, String nextPart, Object value) {
        ensureListSize(list, arrayIndex);
        if (last) {
            list.set(arrayIndex, value);
            return value;
        }
        Object child = list.get(arrayIndex);
        if (child == null) {
            child = createContainer(nextPart);
            list.set(arrayIndex, child);
        }
        return child;
    }

    private Object putToMap(Map<String, Object> map, String key, boolean last, String nextPart, Object value) {
        if (last) {
            map.put(key, value);
            return value;
        }
        return map.computeIfAbsent(key, k -> createContainer(nextPart));
    }

    private void ensureListSize(List<Object> list, int arrayIndex) {
        while (list.size() <= arrayIndex) {
            list.add(null);
        }
    }

    private Object createContainer(String nextPart) {
        if (nextPart != null && isInteger(nextPart)) {
            return new ArrayList<Object>();
        }
        return new LinkedHashMap<String, Object>();
    }

    private static boolean isInteger(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private String preprocessExclamationScalars(String data) {
        String[] lines = data.split("\\R", -1);
        StringBuilder sb = new StringBuilder();
        String ls = "\n";

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            sb.append(preprocessExclamationLine(line));
            if (i < lines.length - 1) {
                sb.append(ls);
            }
        }

        return sb.toString();
    }

    private String preprocessExclamationLine(String line) {
        int colonIdx = line.indexOf(':');
        if (colonIdx < 0) {
            return line;
        }

        String before = line.substring(0, colonIdx + 1);
        String after = line.substring(colonIdx + 1);
        String trimmed = after.trim();

        if (!trimmed.startsWith("!\"")) {
            return line;
        }

        int i = 0;
        while (i < after.length() && Character.isWhitespace(after.charAt(i))) {
            i++;
        }
        String spaces = after.substring(0, i);

        String escaped = trimmed.replace("\"", "\\\"");
        String normalized = "\"" + escaped + "\"";

        return before + spaces + normalized;
    }
}