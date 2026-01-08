package mappers;

import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FlatIni2 implements FlatService {

    private static final String LINE_SEPARATOR = "\n";
    private static final String SECTION_PATTERN = "^\\s*\\[([^\\[\\]]+)]\\s*$";
    private static final String COMMENT_PATTERN = "\\s*#.*$";
    private static final String VALUE_PATTERN = "([^\\s]*)(\\s*)(.*)";
    private static final String META_KEY = "\u0000__flat_ini_meta__";

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        List<String> lines = data.lines().collect(Collectors.toList());
        List<Element> elements = new ArrayList<>();

        Element element = new Element();
        for (String line : lines) {
            if (isSection(line)) {
                element = new Element();
                element.addSection(line);
            } else if (isComment(line)) {
                element.addComment(line);
            } else if (isParameter(line)) {
                element.addParam(line);
            }
            if (!elements.contains(element)) {
                elements.add(element);
            }
        }

        Map<String, FileDataItem> result = new LinkedHashMap<>();
        for (Element elem : elements) {
            List<FileDataItem> items = elem.toFileDataItems();
            for (FileDataItem item : items) {
                result.put(item.getKey(), item);
            }
        }

        FileDataItem meta = createMeta(elements);
        result.put(META_KEY, meta);

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        List<Element> fromMeta = extractMeta(data);
        List<Element> fromData = convert(data);
        List<Element> merged = merge(fromMeta, fromData);
        return deserialize(merged);
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new UnsupportedOperationException("Валидация INI файлов пока не реализована.");
    }

    private List<Element> convert(Map<String, FileDataItem> data) {
        Pattern sectionPattern = Pattern.compile("^(?![^\\[]*\\.[^\\[]*$)(?!.*\\[(?!\\d+]))([^.\\[]+)(?:\\[(\\d+)])?(?:\\.(.*))?$");
        Map<String, TreeMap<Integer, Parameter>> map = new LinkedHashMap<>();
        for (Map.Entry<String, FileDataItem> entry : data.entrySet()) {
            String key = entry.getKey();
            Matcher matcher = sectionPattern.matcher(key);
            matcher.find();
            String section = matcher.group(1);
            int index = matcher.group(2) == null ? -1 : Integer.parseInt(matcher.group(2));
            String paramKey = matcher.group(3) == null ? StringUtils.EMPTY : matcher.group(3);
            String paramValue = entry.getValue().getValue().toString();
            String paramComment = entry.getValue().getComment();
            map.computeIfAbsent(section, k -> new TreeMap<>())
                    .put(index, new Parameter(paramKey + " " + paramValue, paramComment));
        }
        List<Element> result = new ArrayList<>();
        for (String section : map.keySet()) {
            Element element = new Element();
            element.section = section;
            element.type = ElementType.SECTION;
            Collection<Parameter> parameters = map.get(section).values();
            element.params.addAll(parameters);
            result.add(element);
        }
        return result;
    }

    private List<Element> merge(List<Element> fromMeta, List<Element> fromData) {
        fromMeta.removeIf(element -> fromData.stream().noneMatch(x -> x.section.equals(element.section)));
        List<Element> result = new ArrayList<>();
        for (Element metaElement : fromMeta) {
            Element dataElement = fromData.stream()
                    .filter(x -> x.section.equals(metaElement.section))
                    .findFirst()
                    .orElseThrow();
            Element element = synchronize(metaElement, dataElement);
            result.add(element);
        }
        return result;
    }

    private Element synchronize(Element metaElement, Element dataElement) {
        Element result = new Element();
        result.comment = metaElement.comment;
        for (Parameter dataParam : dataElement.params) {
            Parameter metaParam = metaElement.params.stream()
                    .filter(x -> x.key.equals(dataParam.key))
                    .findFirst()
                    .orElse(null);
            if (metaParam != null) {
                Parameter resultParam = new Parameter(dataParam.key + metaParam.separator + dataParam.value, dataParam.comment);
                result.params.add(resultParam);
            }
        }
        result.section = dataElement.section;
        result.type = metaElement.type;
        return result;
    }

    private String deserialize(List<Element> elements) {
        StringJoiner result = new StringJoiner(LINE_SEPARATOR);
        for (Element element : elements) {
            if (element.type == ElementType.SECTION) {
                result.add("[" + element.section + "]");
                for (Parameter param : element.params) {
                    if (StringUtils.isNotEmpty(param.comment)) {
                        result.add(param.comment);
                    }
                    result.add(param.key + param.separator + param.value);
                }
                if (element.comment != null) {
                    result.add(element.comment.toString());
                }
            } else {
                throw new NotImplementedException("NOT A SECTION");
            }
        }

        return result.toString();
    }

    private boolean isSection(String line) {
        return line.matches(SECTION_PATTERN);
    }

    private boolean isComment(String line) {
        return line.matches(COMMENT_PATTERN) || line.isEmpty();
    }

    private boolean isParameter(String line) {
        return !isSection(line) && !isComment(line);
    }

    private FileDataItem createMeta(List<Element> elements) {
        return FileDataItem.builder()
                .key(META_KEY)
                .value(elements)
                .build();
    }

    private List<Element> extractMeta(Map<String, FileDataItem> data) {
        if (!data.containsKey(META_KEY)) {
            return null;
        }
        List<?> list = (List<?>) data.get(META_KEY).getValue();
        return list.stream()
                .map(Element.class::cast)
                .collect(Collectors.toList());
    }

    private static class Element {


        private String section;
        private StringJoiner comment;
        private List<Parameter> params;
        private ElementType type;

        public Element() {
            this.comment = new StringJoiner(LINE_SEPARATOR);
            params = new ArrayList<>();
            type = ElementType.SINGLE;
        }

        public void addSection(String line) {
            Pattern pattern = Pattern.compile(SECTION_PATTERN);
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find()) {
                throw new IllegalStateException("Не удалось извлечь секцию из строки.");
            }
            this.section = matcher.group(1);
            type = ElementType.SECTION;
        }

        public void addComment(String line) {
            if (comment == null) {
                comment = new StringJoiner(LINE_SEPARATOR);
            }
            if (line.isEmpty()) {
                this.comment.add(StringUtils.EMPTY);
                return;
            }
            Pattern pattern = Pattern.compile(COMMENT_PATTERN);
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find()) {
                throw new IllegalStateException("Не удалось извлечь комментарий из строки.");
            }
            String comment = matcher.group();
            this.comment.add(comment);
        }

        public void addParam(String line) {
            String comment = this.comment != null ? this.comment.toString() : StringUtils.EMPTY;
            params.add(new Parameter(line, comment));
            this.comment = null;
        }

        public List<FileDataItem> toFileDataItems() {
            if (params.isEmpty()) {
                FileDataItem item = FileDataItem.builder()
                        .key(section)
                        .value(StringUtils.EMPTY)
                        .build();
                return List.of(item);
            }
            List<FileDataItem> items = new ArrayList<>();
            for (int i = 0; i < params.size(); i++) {
                Parameter param = params.get(i);
                String key;
                if (StringUtils.isEmpty(section)) {
                    key = param.getKey();
                } else {
                    key = section + "[" + i + "]" + (StringUtils.isNotEmpty(param.key) ? "." + param.getKey() : StringUtils.EMPTY);
                }
                FileDataItem item = FileDataItem.builder()
                        .key(key)
                        .value(param.getValue())
                        .comment(param.getComment())
                        .build();
                items.add(item);
            }
            return items;
        }
    }

    @Data
    private static class Parameter {
        private String key;
        private String separator;
        private String value;
        private String comment;

        public Parameter(String line, String comment) {
            line = line.trim();
            Pattern pattern = Pattern.compile(VALUE_PATTERN);
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find()) {
                throw new IllegalStateException("Не удалось выделить параметр.");
            }
            key = matcher.group(1);
            separator = matcher.group(2);
            value = matcher.group(3);
            if (StringUtils.isEmpty(value)) {
                value = key;
                key = StringUtils.EMPTY;
            }
            this.comment = comment;
        }
    }

    private enum ElementType {
        SECTION,
        SINGLE
    }
}
