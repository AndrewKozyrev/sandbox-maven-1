package mappers;

import lombok.NoArgsConstructor;
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
            } else {
                element.addComment(line);
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
            if (META_KEY.equals(key)) {
                continue;
            }
            Matcher matcher = sectionPattern.matcher(key);
            matcher.find();
            String section = matcher.group(1);
            int index = matcher.group(2) == null ? -1 : Integer.parseInt(matcher.group(2));
            String paramKey = matcher.group(3);
            String paramValue = Optional.ofNullable(entry.getValue().getValue())
                    .map(Object::toString)
                    .orElse("");
            String paramComment = entry.getValue().getComment();
            TreeMap<Integer, Parameter> innerMap = map.computeIfAbsent(section, k -> new TreeMap<>());
            String parameterLine = paramKey == null ? paramValue : paramKey + " " + paramValue;
            Parameter parameter;
            if (index == -1 && StringUtils.isEmpty(parameterLine)) {
                parameter = new Parameter();
                parameter.comment = paramComment;
                parameter.key = StringUtils.EMPTY;
                parameter.value = StringUtils.EMPTY;
                parameter.separator = StringUtils.EMPTY;
                parameter.type = ParameterType.EMPTY;
            } else {
                parameter = new Parameter(parameterLine, paramComment, index + 1);
                parameter.type = ParameterType.REGULAR;
            }
            innerMap.put(index, parameter);
        }
        List<Element> result = new ArrayList<>();
        for (String section : map.keySet()) {
            Element element = new Element();
            element.section = section;
            Collection<Parameter> parameters = map.get(section).values();
            element.params.addAll(parameters);
            result.add(element);
        }
        return result;
    }

    private List<Element> merge(List<Element> fromMeta, List<Element> fromData) {
        List<Element> result = new ArrayList<>();
        if (fromMeta != null) {
            fromMeta.removeIf(element -> fromData.stream().noneMatch(x -> x.section.equals(element.section)));
            for (Element metaElement : fromMeta) {
                Element dataElement = fromData.stream()
                        .filter(x -> x.section.equals(metaElement.section))
                        .findFirst()
                        .orElseThrow();
                Element element = synchronize(metaElement, dataElement);
                result.add(element);
            }
        } else {
            result.addAll(fromData);
        }

        return result;
    }

    private Element synchronize(Element metaElement, Element dataElement) {
        Element result = new Element();
        result.sectionComment = metaElement.sectionComment;
        for (Parameter dataParam : dataElement.params) {
            if (dataParam.type == ParameterType.REGULAR) {
                Parameter metaParam = metaElement.params.stream()
                        .filter(x -> x.type == dataParam.type)
                        .filter(x -> x.key.equals(dataParam.key) && x.position == dataParam.position)
                        .findFirst()
                        .orElse(null);
                Parameter resultParam;
                if (metaParam != null) {
                    resultParam = new Parameter(dataParam.key + metaParam.separator + dataParam.value, dataParam.comment, dataParam.position);
                } else {
                    resultParam = new Parameter(dataParam.key + " " + dataParam.value, dataParam.comment, dataParam.position);
                }
                resultParam.type = ParameterType.REGULAR;
                result.params.add(resultParam);
            } else {
                metaElement.params.stream()
                        .filter(x -> x.type != ParameterType.REGULAR)
                        .filter(x -> x.position == dataParam.position)
                        .findFirst()
                        .map(x -> x.type)
                        .ifPresent(type -> dataParam.type = type);
                result.params.add(dataParam);
            }
            result.paramCount++;
        }
        result.section = dataElement.section;
        return result;
    }

    private String deserialize(List<Element> elements) {
        StringJoiner result = new StringJoiner(LINE_SEPARATOR);
        for (Element element : elements) {
            result.add("[" + element.section + "]");
            for (Parameter param : element.params) {
                if (StringUtils.isNotEmpty(param.comment)) {
                    result.add(param.comment);
                }
                if (param.type != ParameterType.END) {
                    result.add(param.key + param.separator + param.value);
                }
            }
            if (element.sectionComment != null) {
                result.add(element.sectionComment.toString());
            }
        }

        return result.toString();
    }

    private boolean isSection(String line) {
        return line.matches(SECTION_PATTERN);
    }

    private boolean isComment(String line) {
        return line.matches(COMMENT_PATTERN);
    }

    private boolean isParameter(String line) {
        return StringUtils.isNotEmpty(line) && !isSection(line) && !isComment(line);
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
        private StringJoiner sectionComment;
        private final List<Parameter> params;
        private int paramCount;

        public Element() {
            params = new ArrayList<>();
            paramCount = 0;
        }

        public void addSection(String line) {
            Pattern pattern = Pattern.compile(SECTION_PATTERN);
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find()) {
                throw new IllegalStateException("Не удалось извлечь секцию из строки.");
            }
            this.section = matcher.group(1);
        }

        public void addComment(String line) {
            if (sectionComment == null) {
                sectionComment = new StringJoiner(LINE_SEPARATOR);
            }
            sectionComment.add(line);
        }

        public void addParam(String line) {
            paramCount++;
            String paramComment;
            paramComment = sectionComment == null ? StringUtils.EMPTY : sectionComment.toString();
            params.add(new Parameter(line, paramComment, paramCount));
            sectionComment = null;
        }

        public List<FileDataItem> toFileDataItems() {
            List<FileDataItem> items = new ArrayList<>();
            if (paramCount == 0) {
                Parameter param = new Parameter();
                if (sectionComment != null) {
                    param.type = ParameterType.EMPTY;
                    param.comment = sectionComment.toString();
                    param.value = StringUtils.EMPTY;
                    sectionComment = null;
                } else {
                    param.type = ParameterType.END;
                }
                params.add(param);
            }
            for (int i = 0; i < params.size(); i++) {
                Parameter param = params.get(i);
                String key;
                if (param.type == ParameterType.REGULAR) {
                    key = section + "[" + i + "]" + (StringUtils.isNotEmpty(param.key) ? "." + param.key : StringUtils.EMPTY);
                } else {
                    key = section;
                }
                FileDataItem item = FileDataItem.builder()
                        .key(key)
                        .value(param.value)
                        .comment(param.comment)
                        .build();
                items.add(item);
            }
            return items;
        }
    }

    @NoArgsConstructor
    private static class Parameter {
        private ParameterType type;
        private String key;
        private String separator;
        private String value;
        private String comment;
        private int position;

        public Parameter(String line, String comment, int position) {
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
            this.position = position;
            type = ParameterType.REGULAR;
        }
    }

    private enum ParameterType {
        REGULAR,
        EMPTY,
        END
    }
}