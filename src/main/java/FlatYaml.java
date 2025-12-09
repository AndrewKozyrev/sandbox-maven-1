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

    private final Yaml yaml;

    public FlatYaml() {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(false);
        options.setIndent(2);
        options.setIndicatorIndent(2);
        options.setIndentWithIndicator(true);

        Representer representer = new QuotedRepresenter(options);
        this.yaml = new Yaml(representer, options);
    }

    @Getter
    private static final class QuotedString {
        private final String value;

        QuotedString(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "\"" + value + "\"";
        }
    }

    private static final class QuotedRepresenter extends Representer {
        QuotedRepresenter(DumperOptions options) {
            super(options);
            this.representers.put(QuotedString.class, new RepresentQuotedString());
        }

        private static class RepresentQuotedString implements Represent {
            @Override
            public Node representData(Object data) {
                QuotedString qs = (QuotedString) data;
                return new ScalarNode(
                        Tag.STR,
                        qs.getValue(),
                        null,
                        null,
                        DumperOptions.ScalarStyle.DOUBLE_QUOTED
                );
            }
        }
    }

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        Node rootNode = yaml.compose(new StringReader(data));
        Map<String, FileDataItem> flatData = new LinkedHashMap<>();
        Map<String, Object> flatDataRaw = flatten(rootNode, "");
        for (Map.Entry<String, Object> item : flatDataRaw.entrySet()) {
            FileDataItem f = new FileDataItem();
            f.setKey(item.getKey());
            f.setValue(item.getValue());
            flatData.put(item.getKey(), f);
        }
        return flatData;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        Map<String, Object> inputMap = new LinkedHashMap<>();
        for (FileDataItem item : data.values()) {
            inputMap.put(item.getKey(), item.getValue());
        }

        Map<String, Object> root = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : inputMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String normalizedKey = key.replaceAll("\\[(\\d+)]", ".$1");
            String[] parts = normalizedKey.split("\\.");
            putPath(root, parts, 0, value);
        }

        return yaml.dump(root);
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Валидация YAML файлов не реализована.");
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

    private void processMappingNode(Map<String, Object> result, MappingNode mappingNode, String parentKey) {
        for (NodeTuple tuple : mappingNode.getValue()) {
            String key = ((ScalarNode) tuple.getKeyNode()).getValue();
            String currentKey = parentKey.isEmpty()
                                ? key
                                : parentKey + "." + key;
            result.putAll(flatten(tuple.getValueNode(), currentKey));
        }
    }

    private void processSequenceNode(Map<String, Object> result, SequenceNode sequenceNode, String parentKey) {
        int index = 0;
        for (Node item : sequenceNode.getValue()) {
            String currentKey = parentKey + "[" + index++ + "]";
            if (item instanceof ScalarNode) {
                addScalarNode(result, (ScalarNode) item, currentKey);
            } else {
                result.putAll(flatten(item, currentKey));
            }
        }
    }

    private void addScalarNode(Map<String, Object> result, ScalarNode scalarNode, String parentKey) {
        DumperOptions.ScalarStyle style = scalarNode.getScalarStyle();
        Tag tag = scalarNode.getTag();
        String text = scalarNode.getValue();
        Object value;
        if (style == DumperOptions.ScalarStyle.DOUBLE_QUOTED
            || style == DumperOptions.ScalarStyle.SINGLE_QUOTED) {
            value = new QuotedString(text);
        } else if (Tag.INT.equals(tag)) {
            try {
                value = Integer.parseInt(text);
            } catch (NumberFormatException e) {
                try {
                    value = Long.parseLong(text);
                } catch (NumberFormatException ex) {
                    value = text;
                }
            }
        } else if (Tag.FLOAT.equals(tag)) {
            try {
                value = Double.parseDouble(text);
            } catch (NumberFormatException e) {
                value = text;
            }
        } else {
            value = text;
        }
        result.put(parentKey, value);
    }


    @SuppressWarnings("unchecked")
    private void putPath(Object container, String[] parts, int index, Object value) {
        String part = parts[index];
        boolean last = (index == parts.length - 1);

        if (isInteger(part)) {
            int arrayIndex = Integer.parseInt(part);
            List<Object> list = (List<Object>) container;

            while (list.size() <= arrayIndex) {
                list.add(null);
            }

            if (last) {
                list.set(arrayIndex, value);
            } else {
                Object child = list.get(arrayIndex);
                if (child == null) {
                    String nextPart = parts[index + 1];
                    Object newChild = isInteger(nextPart)
                                      ? new ArrayList<>()
                                      : new LinkedHashMap<String, Object>();
                    list.set(arrayIndex, newChild);
                    child = newChild;
                }
                putPath(child, parts, index + 1, value);
            }

        } else {
            Map<String, Object> map = (Map<String, Object>) container;

            if (last) {
                map.put(part, value);
            } else {
                Object child = map.get(part);
                if (child == null) {
                    String nextPart = parts[index + 1];
                    Object newChild = isInteger(nextPart)
                                      ? new ArrayList<>()
                                      : new LinkedHashMap<String, Object>();
                    map.put(part, newChild);
                    child = newChild;
                }
                putPath(child, parts, index + 1, value);
            }
        }
    }

    private static boolean isInteger(String s) {
        if (s == null || s.isEmpty()) {return false;}
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
