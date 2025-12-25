import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public class FlatIni implements FlatService {

    private static final String DEFAULT_LS = System.lineSeparator();
    private static final String META_KEY = "\u0000__flat_ini_meta__";
    private static final String META_VERSION = "V1";
    private static final String FIELD_SEP = "\u0001";
    private static final Pattern HOST_LINE_PATTERN = Pattern.compile("^[a-z0-9._-]+\\s+.*$");

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        MetaHidingMap result = new MetaHidingMap();
        if (data == null || data.isBlank()) {
            return result;
        }

        ParseInput input = ParseInput.from(data);
        boolean sectioned = looksLikeSectionedIni(input.lines);

        ParseState st = new ParseState(sectioned);
        for (String line : input.lines) {
            st.consumeLine(line == null ? "" : line, result);
        }
        st.finish(result);

        MetaState meta = new MetaState(sectioned, input.lineSepToken, input.originalEndsWithNewline, st.structure);
        FileDataItem metaItem = new FileDataItem();
        metaItem.setKey(META_KEY);
        metaItem.setValue(encodeMeta(meta));
        result.putInternal(metaItem);

        return result;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        MetaState meta = readMetaState(data);
        if (meta == null || meta.structure == null || meta.structure.isEmpty()) {
            String out = dumpFallback(data);
            return trimTrailingIfNeeded(out, DEFAULT_LS);
        }

        String ls = tokenToLineSep(meta.lineSepToken);
        if (ls == null || ls.isEmpty()) {
            ls = DEFAULT_LS;
        }

        String out = dumpWithStructure(data, meta, ls);
        if (!meta.originalEndsWithNewline) {
            out = trimTrailingIfNeeded(out, ls);
        }
        return out;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new UnsupportedOperationException("Validation of ini/inventory files is not implemented.");
    }

    private static String dumpFallback(Map<String, FileDataItem> data) {
        if (looksLikeSectionedKeys(data.keySet())) {
            return dumpSectionedFallback(data);
        }
        return dumpPlainFallback(data);
    }


    private static String dumpWithStructure(Map<String, FileDataItem> data, MetaState meta, String ls) {
        List<LineEntry> structure = meta.structure;
        Set<String> structureKeys = collectStructureKeys(structure);

        StringBuilder sb = new StringBuilder();
        StructureRenderer renderer = new StructureRenderer(sb, data, meta.sectioned, ls, structure);

        if (!meta.sectioned) {
            for (int i = 0; i < structure.size(); i++) {
                LineEntry e = structure.get(i);
                if (e != null) {
                    renderer.appendStructure(e, i);
                }
            }
            renderer.flushPendingStandalone();
            appendExtraPlainItems(sb, data, structureKeys, ls);
            renderer.finish();
            return sb.toString();
        }

        Map<String, List<ExtraItem>> extrasBySection = collectExtrasSectioned(data, structureKeys);
        Map<String, Integer> insertAtBySection = computeInsertAtBySection(structure);

        for (int i = 0; i <= structure.size(); i++) {
            renderer.flushPendingStandalone();
            appendExtrasAtIndex(sb, data, extrasBySection, insertAtBySection, i, ls);
            if (i < structure.size()) {
                LineEntry e = structure.get(i);
                if (e != null) {
                    renderer.appendStructure(e, i);
                }
            }
        }

        renderer.flushPendingStandalone();
        appendRemainingNewSections(sb, data, extrasBySection, ls);
        renderer.finish();
        return sb.toString();
    }


    private static void appendExtrasAtIndex(
            StringBuilder sb,
            Map<String, FileDataItem> data,
            Map<String, List<ExtraItem>> extrasBySection,
            Map<String, Integer> insertAtBySection,
            int index,
            String ls
    ) {
        if (extrasBySection.isEmpty() || insertAtBySection.isEmpty()) {
            return;
        }

        List<String> sections = new ArrayList<>();
        for (Map.Entry<String, Integer> e : insertAtBySection.entrySet()) {
            Integer v = e.getValue();
            if (v != null && v == index) {
                sections.add(e.getKey());
            }
        }
        if (sections.isEmpty()) {
            return;
        }

        Collections.sort(sections);
        for (String sec : sections) {
            List<ExtraItem> extras = extrasBySection.remove(sec);
            if (extras != null && !extras.isEmpty()) {
                appendExtraItems(sb, data, extras, ls);
            }
        }
    }

    private static void appendExtraItems(StringBuilder sb, Map<String, FileDataItem> data, List<ExtraItem> extras, String ls) {
        for (ExtraItem ex : extras) {
            if (ex == null) {
                continue;
            }
            FileDataItem item = data.get(ex.key);
            if (item != null) {
                appendExtraItem(sb, ex, item, ls);
            }
        }
    }

    private static void appendExtraItem(StringBuilder sb, ExtraItem ex, FileDataItem item, String ls) {
        if (isEmptySectionPlaceholder(ex.key, item)) {
            return;
        }

        String c = item.getComment();
        if (c != null && !c.isBlank()) {
            appendNewComment(sb, c, ls);
        }

        String value = safeValue(item);
        if (ex.host != null && !ex.host.isEmpty()) {
            appendHostLine(sb, ex.host, value, ls);
        } else {
            sb.append(value).append(ls);
        }
    }

    private static void appendRemainingNewSections(StringBuilder sb, Map<String, FileDataItem> data, Map<String, List<ExtraItem>> extrasBySection, String ls) {
        if (extrasBySection.isEmpty()) {
            return;
        }

        List<String> sections = new ArrayList<>(extrasBySection.keySet());
        Collections.sort(sections);

        for (String sec : sections) {
            List<ExtraItem> extras = extrasBySection.get(sec);
            if (extras == null || extras.isEmpty()) {
                continue;
            }

            appendLineSepIfMissing(sb, ls);

            sb.append("[").append(sec).append("]").append(ls);
            appendExtraItems(sb, data, extras, ls);
        }
        extrasBySection.clear();
    }

    private static Map<String, Integer> computeInsertAtBySection(List<LineEntry> structure) {
        Map<String, Integer> insertAt = new LinkedHashMap<>();

        String currentSection = null;
        int sectionHeaderIndex = -1;
        int lastDataIndex = -1;

        for (int i = 0; i < structure.size(); i++) {
            LineEntry e = structure.get(i);
            if (e == null) {
                continue;
            }

            if (e.type == LineType.SECTION) {
                if (currentSection != null) {
                    insertAt.put(currentSection, computeInsertPos(sectionHeaderIndex, lastDataIndex));
                }
                currentSection = extractSectionNameFromHeader(e.text);
                sectionHeaderIndex = i;
                lastDataIndex = -1;
            } else if (e.type == LineType.DATA && currentSection != null) {
                lastDataIndex = i;
            }
        }

        if (currentSection != null) {
            insertAt.put(currentSection, computeInsertPos(sectionHeaderIndex, lastDataIndex));
        }

        return insertAt;
    }

    private static int computeInsertPos(int sectionHeaderIndex, int lastDataIndex) {
        if (lastDataIndex >= 0) {
            return lastDataIndex + 1;
        }
        if (sectionHeaderIndex >= 0) {
            return sectionHeaderIndex + 1;
        }
        return 0;
    }

    private static Set<String> collectStructureKeys(List<LineEntry> structure) {
        Set<String> keys = new LinkedHashSet<>();
        for (LineEntry e : structure) {
            if (e != null && e.type == LineType.DATA && e.key != null && !e.key.isEmpty()) {
                keys.add(e.key);
            }
        }
        return keys;
    }

    private static Map<String, List<ExtraItem>> collectExtrasSectioned(Map<String, FileDataItem> data, Set<String> structureKeys) {
        Map<String, List<ExtraItem>> bySection = new LinkedHashMap<>();

        for (Map.Entry<String, FileDataItem> en : data.entrySet()) {
            String key = en.getKey();
            if (!isExtraCandidateKey(key, structureKeys)) {
                continue;
            }
            SectionKey sk = SectionKey.parse(key);
            if (sk != null) {
                List<ExtraItem> list = bySection.computeIfAbsent(sk.section, k -> new ArrayList<>());
                list.add(new ExtraItem(key, sk.index, sk.host));
            }
        }

        for (Map.Entry<String, List<ExtraItem>> e : bySection.entrySet()) {
            List<ExtraItem> list = e.getValue();
            if (list != null && list.size() > 1) {
                list.sort(Comparator
                        .comparingInt((ExtraItem it) -> it.index)
                        .thenComparing(it -> it.host == null ? "" : it.host)
                        .thenComparing(it -> it.key));
            }
        }

        return bySection;
    }

    private static boolean isExtraCandidateKey(String key, Set<String> structureKeys) {
        if (key == null) {
            return false;
        }
        if (META_KEY.equals(key)) {
            return false;
        }
        return !structureKeys.contains(key);
    }

    private static void appendExtraPlainItems(StringBuilder sb, Map<String, FileDataItem> data, Set<String> structureKeys, String ls) {
        List<String> keys = new ArrayList<>();
        for (String k : data.keySet()) {
            if (k != null && !META_KEY.equals(k) && !structureKeys.contains(k)) {
                keys.add(k);
            }
        }
        if (keys.isEmpty()) {
            return;
        }

        Collections.sort(keys);
        appendLineSepIfMissing(sb, ls);

        for (String k : keys) {
            FileDataItem item = data.get(k);
            if (item != null) {
                appendNewComment(sb, item.getComment(), ls);
                sb.append(k).append("=").append(safeValue(item)).append(ls);
            }
        }
    }


    private static void appendData(
            StringBuilder sb,
            LineEntry e,
            Map<String, FileDataItem> data,
            boolean sectioned,
            String ls,
            List<LineEntry> structure,
            int index
    ) {
        String key = e.key;
        if (key == null) {
            return;
        }

        FileDataItem item = data.get(key);
        if (item == null) {
            return;
        }

        if (sectioned && isEmptySectionPlaceholder(key, item)) {
            return;
        }

        if (shouldInsertNewComment(structure, index, item)) {
            appendNewComment(sb, item.getComment(), ls);
        }

        String value = safeValue(item);
        sb.append(nvl(e.prefix)).append(value).append(nvl(e.suffix)).append(ls);
    }

    private static boolean shouldInsertNewComment(List<LineEntry> structure, int index, FileDataItem item) {
        String c = item == null ? null : item.getComment();
        if (c == null || c.isBlank()) {
            return false;
        }
        return !hasExistingCommentBlock(structure, index);
    }

    private static boolean hasExistingCommentBlock(List<LineEntry> structure, int dataIndex) {
        int i = dataIndex - 1;
        while (i >= 0) {
            LineEntry prev = structure.get(i);
            if (prev == null || prev.type == LineType.EMPTY) {
                i--;
            } else {
                return prev.type == LineType.COMMENT;
            }
        }
        return false;
    }


    private static String stripCommentPrefix(String trimmed) {
        if (trimmed == null) {
            return "";
        }
        String t = trimmed.trim();
        if (t.startsWith("#") || t.startsWith(";")) {
            return t.substring(1).trim();
        }
        return t.trim();
    }

    private static boolean isMetaComment(String content, String section) {
        if (content == null) {
            return false;
        }
        String c = content.toLowerCase(Locale.ROOT);
        if (c.contains("ansible_")) {
            return false;
        }
        if (isChildrenSection(section)) {
            return true;
        }
        return !HOST_LINE_PATTERN.matcher(c).matches();
    }

    private static boolean isChildrenSection(String section) {
        return section != null && section.endsWith(":children");
    }


    private static final class StructureRenderer {
        private final StringBuilder sb;
        private final Map<String, FileDataItem> data;
        private final boolean sectioned;
        private final String ls;
        private final List<LineEntry> structure;

        private final List<PendingLine> pending;
        private String currentSection;

        private StructureRenderer(StringBuilder sb, Map<String, FileDataItem> data, boolean sectioned, String ls, List<LineEntry> structure) {
            this.sb = sb;
            this.data = data;
            this.sectioned = sectioned;
            this.ls = ls;
            this.structure = structure;
            this.pending = new ArrayList<>();
        }

        void appendStructure(LineEntry e, int index) {
            if (e == null) {
                return;
            }
            if (e.type == LineType.SECTION) {
                flushPendingStandalone();
                sb.append(nvl(e.text)).append(ls);
                currentSection = extractSectionNameFromHeader(e.text);
                return;
            }
            if (e.type == LineType.EMPTY) {
                pending.add(PendingLine.empty());
                return;
            }
            if (e.type == LineType.COMMENT) {
                bufferComment(e.text);
                return;
            }
            if (e.type == LineType.DATA) {
                flushPendingForData(e);
                appendData(sb, e, data, sectioned, ls, structure, index);
            }
        }

        void finish() {
            flushPendingStandalone();
        }

        void flushPendingStandalone() {
            if (pending.isEmpty()) {
                return;
            }
            for (PendingLine p : pending) {
                if (p.type == LineType.EMPTY) {
                    sb.append(ls);
                } else if (p.type == LineType.COMMENT) {
                    sb.append(nvl(p.raw)).append(ls);
                }
            }
            pending.clear();
        }

        private void bufferComment(String rawLine) {
            if (rawLine == null) {
                pending.add(PendingLine.comment("", false));
                return;
            }
            String trimmed = rawLine.trim();
            if (!isCommentLine(trimmed)) {
                flushPendingStandalone();
                sb.append(rawLine).append(ls);
                return;
            }

            String content = stripCommentPrefix(trimmed);
            boolean meta = !content.isEmpty() && isMetaComment(content, currentSection);
            pending.add(PendingLine.comment(rawLine, meta));
        }

        private void flushPendingForData(LineEntry dataEntry) {
            if (pending.isEmpty()) {
                return;
            }

            boolean hasAnyComment = false;
            boolean hasMeta = false;
            int firstCommentIdx = -1;
            int lastCommentIdx = -1;
            int firstMetaIdx = -1;
            int lastMetaIdx = -1;

            for (int i = 0; i < pending.size(); i++) {
                PendingLine p = pending.get(i);
                if (p.type != LineType.COMMENT) {
                    continue;
                }
                hasAnyComment = true;
                if (firstCommentIdx < 0) {
                    firstCommentIdx = i;
                }
                lastCommentIdx = i;
                if (p.meta) {
                    hasMeta = true;
                    if (firstMetaIdx < 0) {
                        firstMetaIdx = i;
                    }
                    lastMetaIdx = i;
                }
            }

            if (!hasAnyComment) {
                flushPendingStandalone();
                return;
            }

            FileDataItem item = data.get(dataEntry.key);
            if (item == null) {
                flushPendingForMissingItem(hasMeta);
                pending.clear();
                return;
            }

            String newComment = item.getComment();

            if (hasMeta) {
                String originalMeta = joinPendingComments(true);
                if (newComment == null || newComment.isBlank()) {
                    writePendingExcludingMeta();
                    pending.clear();
                    return;
                }
                if (equalsNullable(originalMeta, newComment)) {
                    flushPendingStandalone();
                    return;
                }

                writePendingReplacingRange(firstMetaIdx, lastMetaIdx, newComment);
                pending.clear();
                return;
            }

            String originalAll = joinPendingComments(false);
            if (newComment == null || newComment.isBlank() || equalsNullable(originalAll, newComment)) {
                flushPendingStandalone();
                return;
            }

            writePendingReplacingAllComments(firstCommentIdx, lastCommentIdx, newComment);
            pending.clear();
        }

        private void flushPendingForMissingItem(boolean hasMeta) {
            if (!hasMeta) {
                flushPendingStandalone();
                return;
            }

            boolean wrote = false;
            for (PendingLine p : pending) {
                if (p.type == LineType.EMPTY) {
                    if (wrote) {
                        sb.append(ls);
                    }
                } else if (p.type == LineType.COMMENT) {
                    if (!p.meta) {
                        sb.append(nvl(p.raw)).append(ls);
                        wrote = true;
                    }
                }
            }
        }

        private void writePendingExcludingMeta() {
            for (PendingLine p : pending) {
                if (p.type == LineType.EMPTY) {
                    sb.append(ls);
                } else if (p.type == LineType.COMMENT) {
                    if (!p.meta) {
                        sb.append(nvl(p.raw)).append(ls);
                    }
                }
            }
        }

        private void writePendingReplacingRange(int startIdx, int endIdx, String newComment) {
            boolean replaced = false;
            for (int i = 0; i < pending.size(); i++) {
                PendingLine p = pending.get(i);
                if (i >= startIdx && i <= endIdx) {
                    if (p.type == LineType.COMMENT && (p.meta)) {
                        if (!replaced) {
                            appendNewComment(sb, newComment, ls);
                            replaced = true;
                        }
                        continue;
                    }
                }

                if (p.type == LineType.EMPTY) {
                    sb.append(ls);
                } else if (p.type == LineType.COMMENT) {
                    if (!(true && p.meta && !replaced && i >= startIdx && i <= endIdx)) {
                        if (!(true && p.meta && i >= startIdx && i <= endIdx)) {
                            sb.append(nvl(p.raw)).append(ls);
                        }
                    }
                }
            }
        }

        private void writePendingReplacingAllComments(int startIdx, int endIdx, String newComment) {
            boolean replaced = false;
            for (int i = 0; i < pending.size(); i++) {
                PendingLine p = pending.get(i);
                if (i >= startIdx && i <= endIdx && p.type == LineType.COMMENT) {
                    if (!replaced) {
                        appendNewComment(sb, newComment, ls);
                        replaced = true;
                    }
                    continue;
                }

                if (p.type == LineType.EMPTY) {
                    sb.append(ls);
                } else if (p.type == LineType.COMMENT) {
                    sb.append(nvl(p.raw)).append(ls);
                }
            }
        }

        private String joinPendingComments(boolean metaOnly) {
            StringBuilder b = new StringBuilder();
            for (PendingLine p : pending) {
                if (p.type != LineType.COMMENT) {
                    continue;
                }
                if (metaOnly && !p.meta) {
                    continue;
                }
                String trimmed = p.raw == null ? "" : p.raw.trim();
                if (!isCommentLine(trimmed)) {
                    continue;
                }
                String content = stripCommentPrefix(trimmed);
                String t = content.trim();
                if (t.isEmpty()) {
                    continue;
                }
                if (b.length() > 0) {
                    b.append(" & ");
                }
                b.append(t);
            }
            return b.toString();
        }

        private static boolean equalsNullable(String a, String b) {
            return Objects.equals(a, b);
        }
    }

    private static final class PendingLine {
        final LineType type;
        final String raw;
        final boolean meta;

        private PendingLine(LineType type, String raw, boolean meta) {
            this.type = type;
            this.raw = raw;
            this.meta = meta;
        }

        static PendingLine empty() {
            return new PendingLine(LineType.EMPTY, null, false);
        }

        static PendingLine comment(String raw, boolean meta) {
            return new PendingLine(LineType.COMMENT, raw, meta);
        }
    }

    private static void appendNewComment(StringBuilder sb, String comment, String ls) {
        if (comment == null || comment.isBlank()) {
            return;
        }
        String[] lines = comment.split("\\R", -1);
        for (String line : lines) {
            if (line == null) {
                continue;
            }
            if (line.isBlank()) {
                sb.append(ls);
            } else if (line.startsWith("#") || line.startsWith(";")) {
                sb.append(line).append(ls);
            } else {
                sb.append("#").append(line).append(ls);
            }
        }
    }

    private static void appendLineSepIfMissing(StringBuilder sb, String ls) {
        if (sb.length() > 0 && ls != null && !ls.isEmpty() && !endsWithLineSep(sb, ls)) {
            sb.append(ls);
        }
    }

    private static void appendHostLine(StringBuilder sb, String host, String value, String ls) {
        if (value == null || value.isEmpty()) {
            sb.append(host).append(ls);
        } else {
            sb.append(host).append(" ").append(value).append(ls);
        }
    }

    private static boolean looksLikeSectionedIni(List<String> lines) {
        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !isCommentLine(trimmed)) {
                return isSectionHeader(trimmed);
            }
        }
        return false;
    }

    private static boolean looksLikeSectionedKeys(Set<String> keys) {
        for (String k : keys) {
            if (k != null && !META_KEY.equals(k) && SectionKey.parse(k) != null) {
                return true;
            }
        }
        return false;
    }

    private static String dumpPlainFallback(Map<String, FileDataItem> data) {
        List<String> keys = new ArrayList<>();
        for (String k : data.keySet()) {
            if (k != null && !META_KEY.equals(k)) {
                keys.add(k);
            }
        }
        Collections.sort(keys);

        StringBuilder sb = new StringBuilder();
        for (String k : keys) {
            FileDataItem item = data.get(k);
            if (item != null) {
                appendNewComment(sb, item.getComment(), FlatIni.DEFAULT_LS);
                sb.append(k).append("=").append(safeValue(item)).append(FlatIni.DEFAULT_LS);
            }
        }
        return trimTrailingIfNeeded(sb.toString(), FlatIni.DEFAULT_LS);
    }

    private static String dumpSectionedFallback(Map<String, FileDataItem> data) {
        List<SectionedItem> items = collectSectionedItems(data);
        sortSectionedItems(items);

        StringBuilder sb = new StringBuilder();
        String currentSection = null;

        for (SectionedItem it : items) {
            if (it == null) {
                continue;
            }
            if (!it.section.equals(currentSection)) {
                currentSection = it.section;
                sb.append("[").append(currentSection).append("]").append(FlatIni.DEFAULT_LS);
            }
            FileDataItem item = data.get(it.originalKey);
            if (item == null) {
                continue;
            }

            appendNewComment(sb, item.getComment(), FlatIni.DEFAULT_LS);
            if (isEmptySectionPlaceholder(it.originalKey, item)) {
                continue;
            }

            String v = safeValue(item);
            if (it.host != null && !it.host.isEmpty()) {
                appendHostLine(sb, it.host, v, FlatIni.DEFAULT_LS);
            } else {
                sb.append(v).append(FlatIni.DEFAULT_LS);
            }
        }

        return trimTrailingIfNeeded(sb.toString(), FlatIni.DEFAULT_LS);
    }

    private static List<SectionedItem> collectSectionedItems(Map<String, FileDataItem> data) {
        List<SectionedItem> items = new ArrayList<>();
        for (Map.Entry<String, FileDataItem> e : data.entrySet()) {
            String k = e.getKey();
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            SectionKey sk = SectionKey.parse(k);
            if (sk != null) {
                items.add(new SectionedItem(k, sk.section, sk.index, sk.host));
            }
        }
        return items;
    }

    private static void sortSectionedItems(List<SectionedItem> items) {
        items.sort(Comparator
                .comparing((SectionedItem it) -> it.section)
                .thenComparingInt(it -> it.index)
                .thenComparing(it -> it.host == null ? "" : it.host));
    }

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.length() >= 3 && trimmed.startsWith("[") && trimmed.endsWith("]");
    }

    private static boolean isCommentLine(String trimmed) {
        return trimmed.startsWith("#") || trimmed.startsWith(";");
    }

    private static boolean isEmptySectionPlaceholder(String key, FileDataItem item) {
        SectionKey sk = SectionKey.parse(key);
        if (sk == null) {
            return false;
        }
        if (sk.host != null) {
            return false;
        }
        if (sk.index != 0) {
            return false;
        }
        return safeValue(item).isEmpty();
    }

    private static String safeValue(FileDataItem item) {
        Object v = item == null ? null : item.getValue();
        return v == null ? "" : String.valueOf(v);
    }

    private static boolean endsWithLineSep(StringBuilder sb, String ls) {
        int n = sb.length();
        int m = ls.length();
        if (n < m) {
            return false;
        }
        for (int i = 0; i < m; i++) {
            if (sb.charAt(n - m + i) != ls.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static String trimTrailingIfNeeded(String text, String ls) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        if (ls != null && !ls.isEmpty() && text.endsWith(ls)) {
            return text.substring(0, text.length() - ls.length());
        }
        int end = text.length();
        while (end > 0) {
            char ch = text.charAt(end - 1);
            if (ch == '\n' || ch == '\r') {
                end--;
            } else {
                break;
            }
        }
        return text.substring(0, end);
    }

    private static String tokenToLineSep(String token) {
        if ("CRLF".equals(token)) {
            return "\r\n";
        }
        if ("CR".equals(token)) {
            return "\r";
        }
        if ("LF".equals(token)) {
            return "\n";
        }
        return DEFAULT_LS;
    }

    private static MetaState readMetaState(Map<String, FileDataItem> data) {
        FileDataItem metaItem = data.get(META_KEY);
        if (metaItem == null) {
            return null;
        }
        Object v = metaItem.getValue();
        if (v == null) {
            return null;
        }
        String b64 = String.valueOf(v);
        if (b64.isEmpty()) {
            return null;
        }
        return decodeMeta(b64);
    }

    private static String encodeMeta(MetaState meta) {
        StringBuilder sb = new StringBuilder();
        sb.append(META_VERSION).append("\n");
        sb.append(meta.sectioned ? "1" : "0").append("\n");
        sb.append(meta.originalEndsWithNewline ? "1" : "0").append("\n");
        sb.append(meta.lineSepToken == null ? "LF" : meta.lineSepToken).append("\n");

        for (int i = 0; i < meta.structure.size(); i++) {
            LineEntry e = meta.structure.get(i);
            if (e == null) {
                continue;
            }
            sb.append(e.type.code).append("|");
            if (e.type == LineType.DATA) {
                String payload = nvl(e.key) + FIELD_SEP + nvl(e.prefix) + FIELD_SEP + nvl(e.suffix);
                sb.append(encodePayload(payload));
            } else if (e.type == LineType.SECTION || e.type == LineType.COMMENT) {
                sb.append(encodePayload(nvl(e.text)));
            }
            sb.append("\n");
        }

        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String encodePayload(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static MetaState decodeMeta(String metaB64) {
        try {
            String text = new String(Base64.getDecoder().decode(metaB64), StandardCharsets.UTF_8);
            String[] raw = text.split("\\R", -1);
            List<String> lines = new ArrayList<>(raw.length);
            Collections.addAll(lines, raw);

            if (lines.size() < 4) {
                return null;
            }
            if (!META_VERSION.equals(lines.get(0))) {
                return null;
            }

            boolean sectioned = "1".equals(lines.get(1));
            boolean endsWithNewline = "1".equals(lines.get(2));
            String token = lines.get(3);
            if (token == null || token.isEmpty()) {
                token = "LF";
            }

            List<LineEntry> structure = new ArrayList<>();
            for (int i = 4; i < lines.size(); i++) {
                LineEntry e = parseEntryLine(lines.get(i));
                if (e != null) {
                    structure.add(e);
                }
            }
            return new MetaState(sectioned, token, endsWithNewline, structure);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static LineEntry parseEntryLine(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }
        int bar = line.indexOf('|');
        if (bar < 0) {
            return null;
        }

        LineType type = LineType.fromCode(line.charAt(0));
        if (type == null) {
            return null;
        }

        String payloadB64 = line.substring(bar + 1);
        String payload = payloadB64.isEmpty() ? "" : decodePayload(payloadB64);

        if (type == LineType.EMPTY) {
            return LineEntry.empty();
        }
        if (type == LineType.SECTION) {
            return LineEntry.section(payload);
        }
        if (type == LineType.COMMENT) {
            return LineEntry.comment(payload);
        }
        if (type == LineType.DATA) {
            return parseDataEntryPayload(payload);
        }
        return null;
    }

    private static LineEntry parseDataEntryPayload(String payload) {
        String p = payload == null ? "" : payload;
        String[] parts = p.split(FIELD_SEP, -1);
        String key = parts.length > 0 ? parts[0] : "";
        String prefix = parts.length > 1 ? parts[1] : "";
        String suffix = parts.length > 2 ? parts[2] : "";
        return key.isEmpty() ? null : LineEntry.data(key, prefix, suffix);
    }

    private static String decodePayload(String payloadB64) {
        return new String(Base64.getDecoder().decode(payloadB64), StandardCharsets.UTF_8);
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    private static String extractSectionNameFromHeader(String rawLine) {
        if (rawLine == null) {
            return null;
        }
        String trimmed = rawLine.trim();
        if (!isSectionHeader(trimmed)) {
            return null;
        }
        String inside = trimmed.substring(1, trimmed.length() - 1).trim();
        return inside.isEmpty() ? null : inside;
    }

    private static String buildSectionedFlatKey(String section, int idx, String host) {
        String base = section + "[" + idx + "]";
        return host == null || host.isEmpty() ? base : base + "." + host;
    }

    private enum LineType {
        SECTION('S'),
        COMMENT('C'),
        EMPTY('E'),
        DATA('D');

        final char code;

        LineType(char code) {
            this.code = code;
        }

        static LineType fromCode(char c) {
            for (LineType t : values()) {
                if (t.code == c) {
                    return t;
                }
            }
            return null;
        }
    }

    private static final class LineEntry {
        final LineType type;
        final String text;
        final String key;
        final String prefix;
        final String suffix;

        private LineEntry(LineType type, String text, String key, String prefix, String suffix) {
            this.type = type;
            this.text = text;
            this.key = key;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        static LineEntry section(String rawLine) {
            return new LineEntry(LineType.SECTION, rawLine, null, null, null);
        }

        static LineEntry comment(String rawLine) {
            return new LineEntry(LineType.COMMENT, rawLine, null, null, null);
        }

        static LineEntry empty() {
            return new LineEntry(LineType.EMPTY, null, null, null, null);
        }

        static LineEntry data(String key, String prefix, String suffix) {
            return new LineEntry(LineType.DATA, null, key, prefix, suffix);
        }
    }

    private static final class ParsedSectionedLine {
        final String host;
        final String value;
        final String prefix;
        final String suffix;

        private ParsedSectionedLine(String host, String value, String prefix, String suffix) {
            this.host = host;
            this.value = value == null ? "" : value;
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
        }
    }

    private static final class MetaState {
        final boolean sectioned;
        final String lineSepToken;
        final boolean originalEndsWithNewline;
        final List<LineEntry> structure;

        private MetaState(boolean sectioned, String lineSepToken, boolean originalEndsWithNewline, List<LineEntry> structure) {
            this.sectioned = sectioned;
            this.lineSepToken = lineSepToken;
            this.originalEndsWithNewline = originalEndsWithNewline;
            this.structure = structure;
        }
    }

    private static final class ExtraItem {
        final String key;
        final int index;
        final String host;

        private ExtraItem(String key, int index, String host) {
            this.key = key;
            this.index = index;
            this.host = host;
        }
    }

    private static final class SectionedItem {
        final String originalKey;
        final String section;
        final int index;
        final String host;

        private SectionedItem(String originalKey, String section, int index, String host) {
            this.originalKey = originalKey;
            this.section = section;
            this.index = index;
            this.host = host;
        }
    }

    private static final class SectionKey {
        final String section;
        final int index;
        final String host;

        private SectionKey(String section, int index, String host) {
            this.section = section;
            this.index = index;
            this.host = host;
        }

        static SectionKey parse(String k) {
            if (k == null) {
                return null;
            }
            int lb = k.indexOf('[');
            int rb = k.indexOf(']');
            if (lb <= 0 || rb <= lb) {
                return null;
            }

            String section = k.substring(0, lb);
            String idxStr = k.substring(lb + 1, rb);
            if (idxStr.isEmpty()) {
                return null;
            }

            int idx;
            try {
                idx = Integer.parseInt(idxStr);
            } catch (NumberFormatException e) {
                return null;
            }

            String host = null;
            if (rb + 1 < k.length() && k.charAt(rb + 1) == '.') {
                String h = k.substring(rb + 2);
                host = h.isEmpty() ? null : h;
            }

            return new SectionKey(section, idx, host);
        }
    }

    private static final class ParseInput {
        final List<String> lines;
        final String lineSepToken;
        final boolean originalEndsWithNewline;

        private ParseInput(List<String> lines, String lineSepToken, boolean originalEndsWithNewline) {
            this.lines = lines;
            this.lineSepToken = lineSepToken;
            this.originalEndsWithNewline = originalEndsWithNewline;
        }

        static ParseInput from(String data) {
            String ls = detectLineSeparator(data);
            boolean ends = endsWithNewline(data);

            String[] raw = data.split("\\R", -1);
            List<String> lines = new ArrayList<>(raw.length);
            Collections.addAll(lines, raw);

            if (ends && !lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
                lines.remove(lines.size() - 1);
            }

            return new ParseInput(lines, lineSepToken(ls), ends);
        }

        private static String detectLineSeparator(String data) {
            if (data.contains("\r\n")) {
                return "\r\n";
            }
            if (data.indexOf('\n') >= 0) {
                return "\n";
            }
            if (data.indexOf('\r') >= 0) {
                return "\r";
            }
            return DEFAULT_LS;
        }

        private static boolean endsWithNewline(String data) {
            return data.endsWith("\n") || data.endsWith("\r");
        }

        private static String lineSepToken(String ls) {
            if ("\r\n".equals(ls)) {
                return "CRLF";
            }
            if ("\r".equals(ls)) {
                return "CR";
            }
            return "LF";
        }
    }

    private static final class ParseState {
        final boolean sectioned;
        String currentSection;
        int indexInSection;
        boolean sectionHasData;
        final List<String> pendingMetaComments;
        final List<LineEntry> structure;

        ParseState(boolean sectioned) {
            this.sectioned = sectioned;
            this.pendingMetaComments = new ArrayList<>();
            this.structure = new ArrayList<>();
        }

        void consumeLine(String rawLine, Map<String, FileDataItem> out) {
            String trimmed = rawLine.trim();

            if (trimmed.isEmpty()) {
                structure.add(LineEntry.empty());
                pendingMetaComments.clear();
                return;
            }

            if (isCommentLine(trimmed)) {
                structure.add(LineEntry.comment(rawLine));
                String content = stripCommentPrefix(trimmed);
                if (!content.isEmpty() && isMetaComment(content, currentSection)) {
                    pendingMetaComments.add(content);
                }
                return;
            }

            if (sectioned && isSectionHeader(trimmed)) {
                finishSectionIfEmpty(out);
                currentSection = extractSectionNameFromHeader(rawLine);
                indexInSection = 0;
                sectionHasData = false;
                pendingMetaComments.clear();
                structure.add(LineEntry.section(rawLine));
                return;
            }

            if (sectioned) {
                addSectionedData(rawLine, out);
            } else {
                addPlainData(rawLine, out);
            }
        }

        void finish(Map<String, FileDataItem> out) {
            finishSectionIfEmpty(out);
        }

        private void addPlainData(String rawLine, Map<String, FileDataItem> out) {
            int eq = rawLine.indexOf('=');
            if (eq < 0) {
                structure.add(LineEntry.comment(rawLine));
                pendingMetaComments.clear();
                return;
            }

            String key = rawLine.substring(0, eq).trim();
            if (key.isEmpty()) {
                structure.add(LineEntry.comment(rawLine));
                pendingMetaComments.clear();
                return;
            }

            String after = rawLine.substring(eq + 1);
            int l = firstNonWhitespace(after);
            int r = lastNonWhitespaceExclusive(after);

            String prefix = rawLine.substring(0, eq + 1) + after.substring(0, l);
            String value = after.substring(l, r);
            String suffix = after.substring(r);

            FileDataItem item = new FileDataItem();
            item.setKey(key);
            item.setValue(value);
            String metaComment = joinMeta();
            if (!metaComment.isEmpty()) {
                item.setComment(metaComment);
            }
            out.put(key, item);

            structure.add(LineEntry.data(key, prefix, suffix));
        }

        private void addSectionedData(String rawLine, Map<String, FileDataItem> out) {
            if (currentSection == null) {
                structure.add(LineEntry.comment(rawLine));
                pendingMetaComments.clear();
                return;
            }

            ParsedSectionedLine pl = parseSectionedLine(rawLine, currentSection);
            String key = buildSectionedFlatKey(currentSection, indexInSection, pl.host);
            indexInSection++;

            FileDataItem item = new FileDataItem();
            item.setKey(key);
            item.setValue(pl.value);
            String metaComment = joinMeta();
            if (!metaComment.isEmpty()) {
                item.setComment(metaComment);
            }
            out.put(key, item);

            structure.add(LineEntry.data(key, pl.prefix, pl.suffix));

            if (!isEmptySectionPlaceholder(key, item)) {
                sectionHasData = true;
            }
        }

        private void finishSectionIfEmpty(Map<String, FileDataItem> out) {
            if (!sectioned || currentSection == null || sectionHasData) {
                return;
            }
            String key = buildSectionedFlatKey(currentSection, 0, null);
            if (!out.containsKey(key)) {
                FileDataItem item = new FileDataItem();
                item.setKey(key);
                item.setValue("");
                out.put(key, item);
            }
            structure.add(LineEntry.data(key, "", ""));
        }

        private String joinMeta() {
            if (pendingMetaComments.isEmpty()) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            for (String s : pendingMetaComments) {
                String t = s == null ? "" : s.trim();
                if (!t.isEmpty()) {
                    if (sb.length() > 0) {
                        sb.append(" & ");
                    }
                    sb.append(t);
                }
            }
            pendingMetaComments.clear();
            return sb.toString();
        }


        private static ParsedSectionedLine parseSectionedLine(String rawLine, String section) {
            int start = firstNonWhitespace(rawLine);
            int endTrim = lastNonWhitespaceExclusive(rawLine);
            if (start >= endTrim) {
                return new ParsedSectionedLine(null, "", "", "");
            }

            int hostEnd = indexOfWhitespace(rawLine, start, endTrim);
            if (hostEnd < 0) {
                String prefix = rawLine.substring(0, start);
                String value = rawLine.substring(start, endTrim);
                String suffix = rawLine.substring(endTrim);
                return new ParsedSectionedLine(null, value, prefix, suffix);
            }

            String first = rawLine.substring(start, hostEnd);
            if (first.contains("=") || isChildrenSection(section)) {
                String prefix = rawLine.substring(0, start);
                String value = rawLine.substring(start, endTrim);
                String suffix = rawLine.substring(endTrim);
                return new ParsedSectionedLine(null, value, prefix, suffix);
            }

            int restStart = firstNonWhitespace(rawLine, hostEnd, endTrim);
            if (restStart >= endTrim) {
                String prefix = rawLine.substring(0, start);
                String value = rawLine.substring(start, endTrim);
                String suffix = rawLine.substring(endTrim);
                return new ParsedSectionedLine(null, value, prefix, suffix);
            }

            String prefix = rawLine.substring(0, restStart);
            String value = rawLine.substring(restStart, endTrim);
            String suffix = rawLine.substring(endTrim);
            return new ParsedSectionedLine(first, value, prefix, suffix);
        }

        private static int indexOfWhitespace(String s, int from, int to) {
            for (int i = from; i < to; i++) {
                if (Character.isWhitespace(s.charAt(i))) {
                    return i;
                }
            }
            return -1;
        }

        private static int firstNonWhitespace(String s) {
            return firstNonWhitespace(s, 0, s.length());
        }

        private static int firstNonWhitespace(String s, int from, int to) {
            for (int i = from; i < to; i++) {
                if (!Character.isWhitespace(s.charAt(i))) {
                    return i;
                }
            }
            return to;
        }

        private static int lastNonWhitespaceExclusive(String s) {
            int end = s.length();
            while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) {
                end--;
            }
            return end;
        }
    }

    private static final class MetaHidingMap extends LinkedHashMap<String, FileDataItem> {

        void putInternal(FileDataItem value) {
            super.put(FlatIni.META_KEY, value);
        }

        @Override
        public boolean containsKey(Object key) {
            return !META_KEY.equals(key) && super.containsKey(key);
        }

        @Override
        public int size() {
            return super.containsKey(META_KEY) ? super.size() - 1 : super.size();
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public Set<String> keySet() {
            Set<String> base = super.keySet();
            if (!base.contains(META_KEY)) {
                return base;
            }
            Set<String> out = new LinkedHashSet<>();
            for (String k : base) {
                if (!META_KEY.equals(k)) {
                    out.add(k);
                }
            }
            return out;
        }

        @Override
        public Collection<FileDataItem> values() {
            if (!super.containsKey(META_KEY)) {
                return super.values();
            }
            List<FileDataItem> out = new ArrayList<>();
            for (Map.Entry<String, FileDataItem> e : super.entrySet()) {
                if (!META_KEY.equals(e.getKey())) {
                    out.add(e.getValue());
                }
            }
            return out;
        }
    }
}