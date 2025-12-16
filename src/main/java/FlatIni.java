import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FlatIni implements FlatService {

    private static final String DEFAULT_LS = System.lineSeparator();

    private final IdentityHashMap<Map<String, FileDataItem>, State> stateByMap = new IdentityHashMap<>();
    private final IdentityHashMap<FileDataItem, State> stateByItem = new IdentityHashMap<>();

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        LinkedHashMap<String, FileDataItem> result = new LinkedHashMap<>();
        if (data == null || data.isBlank()) {
            return result;
        }

        String lineSeparator = detectLineSeparator(data);
        boolean endsWithNewline = data.endsWith("\n") || data.endsWith("\r");

        String[] raw = data.split("\\R", -1);
        List<String> lines = new ArrayList<>();
        Collections.addAll(lines, raw);
        if (endsWithNewline && !lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
            lines.remove(lines.size() - 1);
        }

        boolean sectioned = containsSectionHeader(lines);

        State st = new State(sectioned, endsWithNewline, lineSeparator);
        if (sectioned) {
            parseSectioned(lines, result, st);
        } else {
            parsePlain(lines, result, st);
        }

        if (stateByMap.size() > 64) {
            stateByMap.clear();
        }
        if (stateByItem.size() > 4096) {
            stateByItem.clear();
        }

        stateByMap.put(result, st);
        for (FileDataItem item : result.values()) {
            if (item != null) {
                stateByItem.put(item, st);
            }
        }

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        State st = stateByMap.get(data);
        if (st == null) {
            for (FileDataItem item : data.values()) {
                if (item == null) {
                    continue;
                }
                State s2 = stateByItem.get(item);
                if (s2 != null) {
                    st = s2;
                    break;
                }
            }
        }

        if (st == null) {
            String ls = DEFAULT_LS;
            if (looksLikeSectionedKeys(data)) {
                return dumpSectionedFromKeys(data, ls);
            }
            return dumpPlainFallback(data, ls);
        }

        String out;
        if (st.sectioned) {
            out = dumpWithStructureSectioned(data, st);
        } else {
            out = dumpWithStructurePlain(data, st);
        }

        if (!st.originalEndsWithNewline) {
            String ls = st.lineSeparator;
            if (out.endsWith(ls)) {
                out = out.substring(0, out.length() - ls.length());
            } else if (out.endsWith("\n") || out.endsWith("\r")) {
                out = out.substring(0, out.length() - 1);
            }
        }
        return out;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation of INI files is not implemented yet.");
    }

    private void parsePlain(List<String> lines, LinkedHashMap<String, FileDataItem> result, State st) {
        List<String> pendingMeta = new ArrayList<>();

        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                st.structure.add(LineEntry.empty());
                pendingMeta.clear();
                continue;
            }

            if (isCommentLine(trimmed)) {
                st.structure.add(LineEntry.comment(line));
                String content = stripCommentPrefix(trimmed);
                if (!content.isEmpty() && isMetaComment(content, null)) {
                    pendingMeta.add(content);
                }
                continue;
            }

            int eq = line.indexOf('=');
            if (eq < 0) {
                continue;
            }

            String key = line.substring(0, eq).trim();
            String value = line.substring(eq + 1).trim();

            if (key.isEmpty()) {
                continue;
            }

            FileDataItem item = new FileDataItem();
            item.setKey(key);
            item.setPath(key);
            item.setValue(value);
            if (!pendingMeta.isEmpty()) {
                item.setComment(joinMeta(pendingMeta));
            }
            pendingMeta.clear();

            result.put(key, item);
            st.structure.add(LineEntry.data(key));
        }
    }

    private void parseSectioned(List<String> lines, LinkedHashMap<String, FileDataItem> result, State st) {
        String currentSection = null;
        boolean currentSectionHadData = false;

        Map<String, Integer> sectionIndex = new LinkedHashMap<>();
        List<String> pendingMeta = new ArrayList<>();

        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                st.structure.add(LineEntry.empty());
                pendingMeta.clear();
                continue;
            }

            if (isSectionHeader(trimmed)) {
                if (currentSection != null && !currentSectionHadData) {
                    ensureEmptySectionPlaceholder(result, currentSection);
                }

                currentSection = trimmed.substring(1, trimmed.length() - 1).trim();
                currentSectionHadData = false;
                pendingMeta.clear();

                st.structure.add(LineEntry.section(currentSection));
                if (!sectionIndex.containsKey(currentSection)) {
                    sectionIndex.put(currentSection, 0);
                }
                continue;
            }

            if (isCommentLine(trimmed)) {
                st.structure.add(LineEntry.comment(line));
                String content = stripCommentPrefix(trimmed);
                if (!content.isEmpty() && isMetaComment(content, currentSection)) {
                    pendingMeta.add(content);
                }
                continue;
            }

            if (currentSection == null) {
                continue;
            }

            int idx = sectionIndex.get(currentSection);
            sectionIndex.put(currentSection, idx + 1);

            boolean hasWs = containsWhitespace(trimmed);
            boolean isChildrenSection = currentSection.contains(":children");

            String flatKey;
            String value;

            if (isChildrenSection || !hasWs) {
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
            if (!pendingMeta.isEmpty()) {
                item.setComment(joinMeta(pendingMeta));
            }
            pendingMeta.clear();

            result.put(flatKey, item);
            st.structure.add(LineEntry.data(flatKey));
            currentSectionHadData = true;
        }

        if (currentSection != null && !currentSectionHadData) {
            ensureEmptySectionPlaceholder(result, currentSection);
        }
    }

    private String dumpWithStructurePlain(Map<String, FileDataItem> data, State st) {
        String ls = st.lineSeparator;
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < st.structure.size(); i++) {
            LineEntry e = st.structure.get(i);
            if (e.type == LineType.EMPTY) {
                sb.append(ls);
                continue;
            }
            if (e.type == LineType.COMMENT) {
                sb.append(e.text == null ? "" : e.text).append(ls);
                continue;
            }
            if (e.type == LineType.DATA) {
                FileDataItem item = data.get(e.flatKey);
                if (item == null || item.getKey() == null) {
                    continue;
                }
                String key = item.getKey();
                String value = item.getValue() == null ? "" : item.getValue().toString();
                sb.append(key).append("=").append(value).append(ls);
            }
        }

        return trimTrailingIfNeeded(sb.toString(), st.originalEndsWithNewline, ls);
    }

    private String dumpWithStructureSectioned(Map<String, FileDataItem> data, State st) {
        String ls = st.lineSeparator;
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < st.structure.size(); i++) {
            LineEntry e = st.structure.get(i);
            if (e.type == LineType.EMPTY) {
                sb.append(ls);
                continue;
            }
            if (e.type == LineType.COMMENT) {
                sb.append(e.text == null ? "" : e.text).append(ls);
                continue;
            }
            if (e.type == LineType.SECTION) {
                sb.append("[").append(e.sectionName).append("]").append(ls);
                continue;
            }
            if (e.type == LineType.DATA) {
                FileDataItem item = data.get(e.flatKey);
                if (item == null || item.getKey() == null) {
                    continue;
                }
                sb.append(formatSectionedDataLine(e.flatKey, item)).append(ls);
            }
        }

        return trimTrailingIfNeeded(sb.toString(), st.originalEndsWithNewline, ls);
    }

    private String dumpPlainFallback(Map<String, FileDataItem> data, String ls) {
        List<String> keys = new ArrayList<>(data.keySet());
        keys.sort(Comparator.naturalOrder());

        StringBuilder sb = new StringBuilder();
        boolean first = true;

        for (String k : keys) {
            FileDataItem item = data.get(k);
            if (item == null || item.getKey() == null) {
                continue;
            }

            if (!first) {
                sb.append(ls);
            }

            String comment = item.getComment();
            if (comment != null && !comment.isBlank()) {
                String[] cl = comment.split("\\R");
                for (String c : cl) {
                    if (c != null && !c.isBlank()) {
                        sb.append("#").append(c.trim()).append(ls);
                    }
                }
            }

            String value = item.getValue() == null ? "" : item.getValue().toString();
            sb.append(item.getKey()).append("=").append(value);

            first = false;
        }

        return sb.toString();
    }

    private String dumpSectionedFromKeys(Map<String, FileDataItem> data, String ls) {
        LinkedHashMap<String, List<SectionItem>> bySection = new LinkedHashMap<>();

        for (Map.Entry<String, FileDataItem> e : data.entrySet()) {
            String flatKey = e.getKey();
            if (flatKey == null) {
                continue;
            }
            SectionKey sk = parseSectionKey(flatKey);
            if (sk == null) {
                continue;
            }
            bySection.computeIfAbsent(sk.section, x -> new ArrayList<>())
                    .add(new SectionItem(sk.section, sk.index, flatKey, e.getValue()));
        }

        StringBuilder sb = new StringBuilder();
        boolean firstSection = true;

        for (Map.Entry<String, List<SectionItem>> sec : bySection.entrySet()) {
            String section = sec.getKey();
            List<SectionItem> items = sec.getValue();

            items.sort((a, b) -> {
                if (a.index != b.index) {
                    return Integer.compare(a.index, b.index);
                }
                return a.flatKey.compareTo(b.flatKey);
            });

            if (!firstSection) {
                sb.append(ls);
            }
            firstSection = false;

            sb.append("[").append(section).append("]").append(ls);

            for (SectionItem si : items) {
                if (si.item == null) {
                    continue;
                }

                String value = si.item.getValue() == null ? "" : si.item.getValue().toString();
                if (section.equals(si.section) && si.flatKey.equals(section + "[0]") && value.isEmpty()) {
                    boolean hasNonEmptyInSection = false;
                    for (SectionItem other : items) {
                        if (other.item != null) {
                            String ov = other.item.getValue() == null ? "" : other.item.getValue().toString();
                            if (!ov.isEmpty()) {
                                hasNonEmptyInSection = true;
                                break;
                            }
                        }
                    }
                    if (!hasNonEmptyInSection) {
                        continue;
                    }
                }

                String comment = si.item.getComment();
                if (comment != null && !comment.isBlank()) {
                    String[] cl = comment.split("\\R");
                    for (String c : cl) {
                        if (c != null && !c.isBlank()) {
                            sb.append("#").append(c.trim()).append(ls);
                        }
                    }
                }

                sb.append(formatSectionedValueLine(si.flatKey, value)).append(ls);
            }
        }

        String out = sb.toString();
        if (out.endsWith(ls)) {
            out = out.substring(0, out.length() - ls.length());
        }
        return out;
    }

    private void ensureEmptySectionPlaceholder(LinkedHashMap<String, FileDataItem> result, String section) {
        String key = section + "[0]";
        if (result.containsKey(key)) {
            return;
        }
        FileDataItem item = new FileDataItem();
        item.setKey(key);
        item.setPath(key);
        item.setValue("");
        result.put(key, item);
    }

    private String formatSectionedDataLine(String flatKey, FileDataItem item) {
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

    private String formatSectionedValueLine(String flatKey, String value) {
        int dotAfterBracket = flatKey.indexOf("].");
        if (dotAfterBracket >= 0) {
            String host = flatKey.substring(dotAfterBracket + 2);
            if (value == null || value.isEmpty()) {
                return host;
            }
            return host + " " + value;
        }
        return value == null ? "" : value;
    }

    private boolean looksLikeSectionedKeys(Map<String, FileDataItem> data) {
        for (String k : data.keySet()) {
            if (k != null && k.indexOf('[') > 0 && k.indexOf(']') > k.indexOf('[')) {
                return true;
            }
        }
        return false;
    }

    private boolean containsSectionHeader(List<String> lines) {
        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String t = line.trim();
            if (isSectionHeader(t)) {
                return true;
            }
        }
        return false;
    }

    private boolean isSectionHeader(String trimmed) {
        return trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.length() >= 2;
    }

    private boolean isCommentLine(String trimmed) {
        return trimmed.startsWith("#") || trimmed.startsWith(";");
    }

    private String stripCommentPrefix(String trimmed) {
        if (trimmed == null || trimmed.isEmpty()) {
            return "";
        }
        char c = trimmed.charAt(0);
        if (c == '#' || c == ';') {
            return trimmed.substring(1).trim();
        }
        return trimmed.trim();
    }

    private String joinMeta(List<String> meta) {
        if (meta == null || meta.isEmpty()) {
            return null;
        }
        if (meta.size() == 1) {
            return meta.get(0);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < meta.size(); i++) {
            if (i > 0) {
                sb.append(" & ");
            }
            sb.append(meta.get(i));
        }
        return sb.toString();
    }

    private boolean isMetaComment(String content, String currentSection) {
        if (content == null) {
            return false;
        }
        String s = content.trim();
        if (s.isEmpty()) {
            return false;
        }
        return !isLikelyCommentedOutDataLine(s, currentSection);
    }

    private boolean isLikelyCommentedOutDataLine(String s, String currentSection) {
        String first = firstToken(s);
        if (first.isEmpty()) {
            return false;
        }
        boolean hasDot = first.indexOf('.') >= 0;
        boolean hasWs = containsWhitespace(s);

        if (currentSection != null && currentSection.contains(":children")) {
            return false;
        }

        if (hasDot && !hasWs) {
            return true;
        }
        if (hasDot) {
            return true;
        }
        return s.contains("ansible_") || s.contains("ansible_host") || s.contains("ansibleHost");
    }

    private String firstToken(String s) {
        int ws = firstWhitespaceIndex(s);
        if (ws < 0) {
            return s.trim();
        }
        return s.substring(0, ws).trim();
    }

    private boolean containsWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private int firstWhitespaceIndex(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private String detectLineSeparator(String data) {
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

    private String trimTrailingIfNeeded(String s, boolean endsWithNewline, String ls) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        if (endsWithNewline) {
            return s;
        }
        if (s.endsWith(ls)) {
            return s.substring(0, s.length() - ls.length());
        }
        if (s.endsWith("\n") || s.endsWith("\r")) {
            return s.substring(0, s.length() - 1);
        }
        return s;
    }

    private SectionKey parseSectionKey(String flatKey) {
        int lb = flatKey.indexOf('[');
        int rb = flatKey.indexOf(']');
        if (lb <= 0 || rb <= lb) {
            return null;
        }
        String section = flatKey.substring(0, lb);
        String idx = flatKey.substring(lb + 1, rb);
        int index;
        try {
            index = Integer.parseInt(idx);
        } catch (NumberFormatException ex) {
            return null;
        }
        return new SectionKey(section, index);
    }

    private static final class SectionKey {
        final String section;
        final int index;

        SectionKey(String section, int index) {
            this.section = section;
            this.index = index;
        }
    }

    private static final class SectionItem {
        final String section;
        final int index;
        final String flatKey;
        final FileDataItem item;

        SectionItem(String section, int index, String flatKey, FileDataItem item) {
            this.section = section;
            this.index = index;
            this.flatKey = flatKey;
            this.item = item;
        }
    }

    private enum LineType {
        EMPTY,
        COMMENT,
        SECTION,
        DATA
    }

    private static final class LineEntry {
        final LineType type;
        final String text;
        final String sectionName;
        final String flatKey;

        private LineEntry(LineType type, String text, String sectionName, String flatKey) {
            this.type = type;
            this.text = text;
            this.sectionName = sectionName;
            this.flatKey = flatKey;
        }

        static LineEntry empty() {
            return new LineEntry(LineType.EMPTY, null, null, null);
        }

        static LineEntry comment(String line) {
            return new LineEntry(LineType.COMMENT, line, null, null);
        }

        static LineEntry section(String name) {
            return new LineEntry(LineType.SECTION, null, name, null);
        }

        static LineEntry data(String flatKey) {
            return new LineEntry(LineType.DATA, null, null, flatKey);
        }
    }

    private static final class State {
        final boolean sectioned;
        final boolean originalEndsWithNewline;
        final String lineSeparator;
        final List<LineEntry> structure = new ArrayList<>();

        State(boolean sectioned, boolean originalEndsWithNewline, String lineSeparator) {
            this.sectioned = sectioned;
            this.originalEndsWithNewline = originalEndsWithNewline;
            this.lineSeparator = lineSeparator == null ? DEFAULT_LS : lineSeparator;
        }
    }
}