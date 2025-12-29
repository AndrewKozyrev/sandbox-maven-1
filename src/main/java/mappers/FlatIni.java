package mappers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("java:S1220")
public class FlatIni implements FlatService {

    private static final String META_KEY = "\u0000__flat_ini_meta__";
    private static final String META_VERSION = "flat-ini-meta-v1";
    private static final String TOKEN_RN = "RN";
    private static final String TOKEN_N = "N";
    private static final String TOKEN_R = "R";

    @Override
    public Map<String, FileDataItem> flatToMap(String data) {
        ParsedText parsed = ParsedText.from(data == null ? "" : data);
        ParseContext ctx = new ParseContext(parsed);

        for (String rawLine : parsed.lines) {
            processRawLine(ctx, rawLine);
        }

        return ctx.toResult();
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        MetaState meta = readMeta(data);
        if (isMissingStructure(meta)) {
            return dumpFallback(data, System.lineSeparator());
        }

        Map<String, FileDataItem> filtered = filterEmptySectionPlaceholders(data, meta);
        String ls = tokenToLineSep(meta.lineSepToken);
        String out = new DumpContext(filtered, meta, ls).dump();

        int desiredSeps;
        if (!meta.endsWithNewline) {
            desiredSeps = 0;
        } else {
            desiredSeps = countTrailingEmpty(meta.structure) + 1;
        }
        return adjustTrailingLineSeparators(out, ls, desiredSeps);
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new UnsupportedOperationException("Validation of .ini files is not implemented yet.");
    }

    private static Map<String, FileDataItem> filterEmptySectionPlaceholders(Map<String, FileDataItem> data,
                                                                            MetaState meta) {
        if (data == null || data.isEmpty() || meta == null || meta.structure == null) {
            return data;
        }

        Set<String> sectionNames = new HashSet<>();
        for (LineEntry e : meta.structure) {
            if (e != null && e.type == LineType.SECTION) {
                String raw = e.text == null ? "" : e.text.trim();
                String name = extractSectionNameFromHeader(raw);
                if (name != null && !name.isEmpty()) {
                    sectionNames.add(name);
                }
            }
        }
        if (sectionNames.isEmpty()) {
            return data;
        }

        Map<String, FileDataItem> result = new LinkedHashMap<>(data.size());
        for (Map.Entry<String, FileDataItem> entry : data.entrySet()) {
            String k = entry.getKey();
            FileDataItem v = entry.getValue();
            if (k != null
                    && !META_KEY.equals(k)
                    && sectionNames.contains(k)
                    && ParsedKey.parse(k).section == null) {
                continue;
            }
            result.put(k, v);
        }
        return result;
    }

    private static void processRawLine(ParseContext ctx, String rawLine) {
        if (ctx == null) {
            return;
        }

        String trimmed = rawLine.trim();
        if (trimmed.isEmpty()) {
            ctx.onEmptyLine();
            return;
        }

        if (isSectionHeader(trimmed)) {
            ctx.onSectionHeader(rawLine, trimmed);
            return;
        }

        String leftTrimmed = stripLeading(rawLine);
        if (isCommentLine(leftTrimmed)) {
            ctx.onCommentLine(rawLine);
            return;
        }

        ctx.onDataOrUnknownLine(rawLine);
    }

    private static boolean isMissingStructure(MetaState meta) {
        return meta == null || meta.structure == null || meta.structure.isEmpty();
    }

    private static final class DumpContext {
        private final Map<String, FileDataItem> items;
        private final Set<String> metaKeys;
        private final Map<String, List<String>> extrasBySection;
        private final Set<String> printed;
        private final List<Segment> segments;
        private final String ls;
        private final StringBuilder out;
        private final SegmentRenderer renderer;

        DumpContext(Map<String, FileDataItem> items, MetaState meta, String ls) {
            this.items = items == null ? Collections.emptyMap() : items;
            List<LineEntry> structure = meta == null || meta.structure == null ? Collections.emptyList() : meta.structure;
            this.segments = splitSegments(structure);
            this.metaKeys = collectMetaKeys(structure);
            this.extrasBySection = computeExtras(this.items, this.metaKeys);
            this.printed = new HashSet<>();
            this.ls = ls == null ? System.lineSeparator() : ls;
            this.out = new StringBuilder();
            this.renderer = new SegmentRenderer(this.out, this.items, this.printed, this.ls);
        }

        String dump() {
            for (Segment seg : segments) {
                renderSegmentOrEmpty(seg);
            }
            appendRemainingExtras();
            return out.toString();
        }

        private void renderSegmentOrEmpty(Segment seg) {
            List<String> extras = extrasForSegment(seg);
            if (shouldRenderOnlyEmptyLines(seg, extras)) {
                renderer.renderOnlyEmptyLines(seg);
                return;
            }
            renderer.renderSegment(seg, extras);
        }

        private List<String> extrasForSegment(Segment seg) {
            if (extrasBySection == null) {
                return Collections.emptyList();
            }
            String sec = seg == null ? null : seg.section;
            List<String> extras = extrasBySection.get(sec);
            return extras == null ? Collections.emptyList() : extras;
        }

        private boolean shouldRenderOnlyEmptyLines(Segment seg, List<String> extras) {
            boolean originallyHadData = segmentOriginallyHasData(seg);
            boolean hasAnyNow = segmentHasAnyDataNow(seg, items);
            boolean hasExtras = extras != null && !extras.isEmpty();
            return originallyHadData && !hasAnyNow && !hasExtras;
        }

        private void appendRemainingExtras() {
            GroupedKeys keys = collectUnprintedKeys(items, printed, metaKeys);
            appendRootEntries(out, items, keys.rootKeys, printed, ls);
            appendSectionEntries(out, items, keys.bySection, printed, ls);
        }

        private static Set<String> collectMetaKeys(List<LineEntry> structure) {
            Set<String> keys = new HashSet<>();
            if (structure == null) {
                return keys;
            }
            for (LineEntry e : structure) {
                if (e != null && e.type == LineType.DATA && e.key != null) {
                    keys.add(e.key);
                }
            }
            return keys;
        }
    }

    private static GroupedKeys collectUnprintedKeys(Map<String, FileDataItem> items,
                                                    Set<String> printedKeys,
                                                    Set<String> metaKeys) {
        Map<String, List<String>> bySection = new HashMap<>();
        List<String> rootKeys = new ArrayList<>();

        if (items == null) {
            return new GroupedKeys(rootKeys, bySection);
        }

        for (String k : items.keySet()) {
            if (!shouldSkipAsExtra(k, printedKeys, metaKeys)) {
                ParsedKey pk = ParsedKey.parse(k);
                if (pk.section == null) {
                    rootKeys.add(k);
                } else {
                    bySection.computeIfAbsent(pk.section, x -> new ArrayList<>()).add(k);
                }
            }
        }

        rootKeys.sort(String::compareTo);

        return new GroupedKeys(rootKeys, bySection);
    }

    private static boolean shouldSkipAsExtra(String key, Set<String> printedKeys, Set<String> metaKeys) {
        if (key == null) {
            return true;
        }
        if (META_KEY.equals(key)) {
            return true;
        }
        if (metaKeys != null && metaKeys.contains(key)) {
            return true;
        }
        return printedKeys != null && printedKeys.contains(key);
    }

    private static void appendRootEntries(StringBuilder out,
                                          Map<String, FileDataItem> items,
                                          List<String> rootKeys,
                                          Set<String> printedKeys,
                                          String ls) {
        if (items == null || rootKeys == null || rootKeys.isEmpty()) {
            return;
        }
        for (String k : rootKeys) {
            FileDataItem item = items.get(k);
            if (item != null) {
                appendCommentBlock(out, item.getComment(), ls);
                out.append(k).append('=').append(safeValue(item)).append(ls);
                if (printedKeys != null) {
                    printedKeys.add(k);
                }
            }
        }
    }

    private static void appendSectionEntries(StringBuilder out,
                                             Map<String, FileDataItem> items,
                                             Map<String, List<String>> bySection,
                                             Set<String> printedKeys,
                                             String ls) {
        if (items == null || bySection == null || bySection.isEmpty()) {
            return;
        }

        List<String> sectionNames = new ArrayList<>(bySection.keySet());
        sectionNames.sort(String::compareTo);

        for (String section : sectionNames) {
            if (out.length() > 0) {
                out.append(ls);
            }
            out.append('[').append(section).append(']').append(ls);

            List<String> keys = bySection.get(section);
            if (keys != null && !keys.isEmpty()) {
                keys.sort(sectionKeyComparator());
                for (String k : keys) {
                    FileDataItem item = items.get(k);
                    boolean printable = item != null && !META_KEY.equals(k);
                    if (printable) {
                        appendCommentBlock(out, item.getComment(), ls);
                        appendSectionLine(out, ParsedKey.parse(k), safeValue(item), ls);
                        if (printedKeys != null) {
                            printedKeys.add(k);
                        }
                    }
                }
            }
        }
    }

    private static void appendSectionLine(StringBuilder out, ParsedKey pk, String value, String ls) {
        if (pk == null || pk.host == null) {
            out.append(value).append(ls);
            return;
        }
        out.append(pk.host);
        if (value != null && !value.isEmpty()) {
            out.append(' ').append(value);
        }
        out.append(ls);
    }

    private static String dumpFallback(Map<String, FileDataItem> items, String ls) {
        GroupedKeys keys = collectAllKeys(items);
        StringBuilder sb = new StringBuilder();
        appendRootEntries(sb, items, keys.rootKeys, null, ls);
        appendFallbackSections(sb, items, keys.bySection, ls);
        return trimFinalLineSep(sb.toString(), ls);
    }

    private static GroupedKeys collectAllKeys(Map<String, FileDataItem> items) {
        Map<String, List<String>> bySection = new HashMap<>();
        List<String> rootKeys = new ArrayList<>();

        if (items == null) {
            return new GroupedKeys(rootKeys, bySection);
        }

        for (String k : items.keySet()) {
            boolean include = k != null && !META_KEY.equals(k);
            if (include) {
                ParsedKey pk = ParsedKey.parse(k);
                if (pk.section == null) {
                    rootKeys.add(k);
                } else {
                    bySection.computeIfAbsent(pk.section, x -> new ArrayList<>()).add(k);
                }
            }
        }

        rootKeys.sort(String::compareTo);

        return new GroupedKeys(rootKeys, bySection);
    }

    private static void appendFallbackSections(StringBuilder sb,
                                               Map<String, FileDataItem> items,
                                               Map<String, List<String>> bySection,
                                               String ls) {
        if (items == null || bySection == null || bySection.isEmpty()) {
            return;
        }

        List<String> sectionNames = new ArrayList<>(bySection.keySet());
        sectionNames.sort(String::compareTo);

        for (String section : sectionNames) {
            if (sb.length() > 0) {
                sb.append(ls);
            }
            sb.append('[').append(section).append(']').append(ls);

            List<String> list = bySection.get(section);
            if (list != null && !list.isEmpty()) {
                list.sort(sectionKeyComparator());
                for (String k : list) {
                    FileDataItem item = items.get(k);
                    if (item != null) {
                        appendCommentBlock(sb, item.getComment(), ls);
                        appendSectionLine(sb, ParsedKey.parse(k), safeValue(item), ls);
                    }
                }
            }
        }
    }

    private static Comparator<String> sectionKeyComparator() {
        return (a, b) -> {
            ParsedKey pa = ParsedKey.parse(a);
            ParsedKey pb = ParsedKey.parse(b);
            int c = Integer.compare(pa.index, pb.index);
            if (c != 0) {
                return c;
            }
            String ha = pa.host == null ? "" : pa.host;
            String hb = pb.host == null ? "" : pb.host;
            return ha.compareTo(hb);
        };
    }

    private static final class SegmentRenderer {
        private final StringBuilder out;
        private final Map<String, FileDataItem> items;
        private final Set<String> printedKeys;
        private final String ls;

        SegmentRenderer(StringBuilder out, Map<String, FileDataItem> items, Set<String> printedKeys, String ls) {
            this.out = out == null ? new StringBuilder() : out;
            this.items = items == null ? Collections.emptyMap() : items;
            this.printedKeys = printedKeys == null ? new HashSet<>() : printedKeys;
            this.ls = ls == null ? System.lineSeparator() : ls;
        }

        void renderSegment(Segment seg, List<String> extras) {
            List<LineEntry> entries = seg == null || seg.entries == null ? Collections.emptyList() : seg.entries;
            int trailingEmpty = countTrailingEmpty(entries);
            int limit = entries.size() - trailingEmpty;

            List<String> pendingComments = new ArrayList<>();

            for (int i = 0; i < limit; i++) {
                LineEntry e = entries.get(i);
                if (e != null) {
                    renderEntry(pendingComments, e);
                }
            }

            flushRawComments(pendingComments);
            pendingComments.clear();

            renderExtras(extras);
            appendTrailingEmptyLines(trailingEmpty);
        }

        void renderOnlyEmptyLines(Segment seg) {
            if (seg == null || seg.entries == null) {
                return;
            }
            for (LineEntry e : seg.entries) {
                if (e != null && e.type == LineType.EMPTY) {
                    out.append(ls);
                }
            }
        }

        private void renderEntry(List<String> pendingComments, LineEntry e) {
            if (e.type == LineType.COMMENT) {
                pendingComments.add(e.text == null ? "" : e.text);
                return;
            }
            if (e.type == LineType.EMPTY) {
                flushRawComments(pendingComments);
                out.append(ls);
                pendingComments.clear();
                return;
            }
            if (e.type == LineType.SECTION) {
                flushRawComments(pendingComments);
                out.append(e.text == null ? "" : e.text).append(ls);
                pendingComments.clear();
                return;
            }
            if (e.type == LineType.DATA) {
                renderDataEntry(pendingComments, e);
            }
        }

        private void renderDataEntry(List<String> pendingComments, LineEntry e) {
            String key = e.key;
            FileDataItem item = key == null ? null : items.get(key);

            if (item == null || META_KEY.equals(key)) {
                pendingComments.clear();
                return;
            }

            String originalBlock = joinWithLf(pendingComments);
            appendUpdatedComment(pendingComments, originalBlock, item.getComment());

            pendingComments.clear();
            printedKeys.add(key);

            String value = safeValue(item);
            out.append(nvl(e.prefix)).append(value).append(nvl(e.suffix)).append(ls);
        }

        private void appendUpdatedComment(List<String> pendingComments, String originalBlock, String newComment) {
            if (newComment == null || newComment.isEmpty()) {
                return;
            }
            if (newComment.equals(originalBlock)) {
                flushRawComments(pendingComments);
                return;
            }
            appendCommentBlock(out, newComment, ls);
        }

        private void renderExtras(List<String> extras) {
            if (extras == null || extras.isEmpty()) {
                return;
            }

            extras.sort(Comparator.comparingInt(a -> ParsedKey.parse(a).index));

            for (String k : extras) {
                FileDataItem item = items.get(k);
                if (item != null && !META_KEY.equals(k)) {
                    appendCommentBlock(out, item.getComment(), ls);
                    appendSectionLine(out, ParsedKey.parse(k), safeValue(item), ls);
                    printedKeys.add(k);
                }
            }
        }

        private void appendTrailingEmptyLines(int trailingEmpty) {
            if (trailingEmpty <= 0) {
                return;
            }
            out.append(String.valueOf(ls).repeat(trailingEmpty));
        }

        private void flushRawComments(List<String> pending) {
            if (pending == null || pending.isEmpty()) {
                return;
            }
            for (String line : pending) {
                out.append(line == null ? "" : line).append(ls);
            }
        }
    }

    private static Map<String, List<String>> computeExtras(Map<String, FileDataItem> items, Set<String> metaKeys) {
        Map<String, List<String>> bySection = new HashMap<>();
        if (items == null) {
            return bySection;
        }
        for (String k : items.keySet()) {
            boolean include = k != null && !META_KEY.equals(k) && (metaKeys == null || !metaKeys.contains(k));
            if (include) {
                ParsedKey pk = ParsedKey.parse(k);
                bySection.computeIfAbsent(pk.section, x -> new ArrayList<>()).add(k);
            }
        }
        return bySection;
    }

    private static boolean segmentOriginallyHasData(Segment seg) {
        if (seg == null || seg.entries == null) {
            return false;
        }
        for (LineEntry e : seg.entries) {
            if (e != null && e.type == LineType.DATA) {
                return true;
            }
        }
        return false;
    }

    private static boolean segmentHasAnyDataNow(Segment seg, Map<String, FileDataItem> items) {
        if (seg == null || seg.entries == null) {
            return false;
        }
        for (LineEntry e : seg.entries) {
            if (e != null && e.type == LineType.DATA) {
                String k = e.key;
                boolean present = k != null && !META_KEY.equals(k) && items.get(k) != null;
                if (present) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<Segment> splitSegments(List<LineEntry> structure) {
        if (structure == null || structure.isEmpty()) {
            return Collections.singletonList(new Segment(null, Collections.emptyList()));
        }
        List<Segment> out = new ArrayList<>();
        List<LineEntry> current = new ArrayList<>();
        String currentSection = null;

        for (LineEntry e : structure) {
            if (e != null && e.type == LineType.SECTION) {
                if (!current.isEmpty() || out.isEmpty()) {
                    out.add(new Segment(currentSection, current));
                }
                current = new ArrayList<>();
                current.add(e);
                currentSection = extractSectionNameFromHeader(e.text == null ? "" : e.text.trim());
            } else {
                current.add(e);
            }
        }
        out.add(new Segment(currentSection, current));
        return out;
    }

    private static int countTrailingEmpty(List<LineEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        int n = entries.size();
        int c = 0;
        for (int i = n - 1; i >= 0; i--) {
            LineEntry e = entries.get(i);
            if (e != null && e.type == LineType.EMPTY) {
                c++;
            } else {
                break;
            }
        }
        return c;
    }

    private static void appendCommentBlock(StringBuilder out, String comment, String ls) {
        if (comment == null || comment.isEmpty()) {
            return;
        }
        String normalized = normalizeLf(comment);
        String[] parts = normalized.split("\n", -1);
        for (String p : parts) {
            if (p.isEmpty()) {
                out.append(ls);
            } else {
                appendCommentLine(out, p, ls);
            }
        }
    }

    private static void appendCommentLine(StringBuilder out, String line, String ls) {
        String t = stripLeading(line);
        if (t.startsWith("#") || t.startsWith(";")) {
            out.append(line).append(ls);
        } else {
            out.append('#').append(line).append(ls);
        }
    }

    private static String encodeMeta(MetaState meta) {
        StringBuilder sb = new StringBuilder();
        sb.append(META_VERSION).append('\n');
        sb.append(meta.lineSepToken == null ? TOKEN_N : meta.lineSepToken).append('\n');
        sb.append(meta.endsWithNewline ? '1' : '0').append('\n');
        int n = meta.structure == null ? 0 : meta.structure.size();
        sb.append(n).append('\n');

        if (n == 0) {
            return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
        }

        for (LineEntry e : meta.structure) {
            appendMetaLine(sb, e);
        }
        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void appendMetaLine(StringBuilder sb, LineEntry e) {
        if (e == null || e.type == LineType.EMPTY) {
            sb.append('E').append('\n');
            return;
        }
        if (e.type == LineType.SECTION) {
            sb.append('S').append('\t').append(b64(nvl(e.text))).append('\n');
            return;
        }
        if (e.type == LineType.COMMENT) {
            sb.append('C').append('\t').append(b64(nvl(e.text))).append('\n');
            return;
        }
        if (e.type == LineType.DATA) {
            sb.append('D').append('\t')
                    .append(b64(nvl(e.key))).append('\t')
                    .append(b64(nvl(e.prefix))).append('\t')
                    .append(b64(nvl(e.suffix))).append('\n');
            return;
        }
        sb.append('E').append('\n');
    }

    private static MetaState decodeMeta(String metaB64) {
        String payload = decodeMetaPayload(metaB64);
        if (payload == null) {
            return null;
        }
        String[] lines = normalizeLf(payload).split("\n", -1);
        return parseMetaPayload(lines);
    }

    private static String decodeMetaPayload(String metaB64) {
        if (metaB64 == null || metaB64.isEmpty()) {
            return null;
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(metaB64);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static MetaState parseMetaPayload(String[] lines) {
        if (lines == null || lines.length < 4) {
            return null;
        }
        if (!META_VERSION.equals(lines[0])) {
            return null;
        }

        MetaState meta = new MetaState();
        meta.lineSepToken = lines[1] == null || lines[1].isEmpty() ? TOKEN_N : lines[1];
        meta.endsWithNewline = "1".equals(lines[2]);

        Integer count = parseMetaCount(lines[3]);
        if (count == null) {
            return null;
        }

        meta.structure = parseMetaStructure(lines, count);
        return meta;
    }

    private static Integer parseMetaCount(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static List<LineEntry> parseMetaStructure(String[] lines, int count) {
        List<LineEntry> structure = new ArrayList<>();
        int idx = 4;

        while (idx < lines.length && structure.size() < count) {
            String line = lines[idx++];
            if (!line.isEmpty()) {
                LineEntry entry = parseMetaLine(line);
                if (entry != null) {
                    structure.add(entry);
                }
            }
        }

        return structure;
    }

    private static LineEntry parseMetaLine(String line) {
        char c = line.charAt(0);
        if (c == 'E') {
            return LineEntry.empty();
        }

        int t = line.indexOf('\t');
        String p = t < 0 ? "" : line.substring(t + 1);

        if (c == 'S') {
            return LineEntry.section(unb64(p));
        }
        if (c == 'C') {
            return LineEntry.comment(unb64(p));
        }
        if (c == 'D') {
            String[] parts = line.split("\t", -1);
            String key = parts.length > 1 ? unb64(parts[1]) : "";
            String pref = parts.length > 2 ? unb64(parts[2]) : "";
            String suf = parts.length > 3 ? unb64(parts[3]) : "";
            return LineEntry.data(key, pref, suf);
        }
        return null;
    }

    private static MetaState readMeta(Map<String, FileDataItem> data) {
        FileDataItem metaItem = data.get(META_KEY);
        if (metaItem == null) {
            return null;
        }
        Object v = metaItem.getValue();
        if (v == null) {
            return null;
        }
        String metaB64 = v.toString();
        if (metaB64.isEmpty()) {
            return null;
        }
        return decodeMeta(metaB64);
    }

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.length() >= 2;
    }

    private static String extractSectionNameFromHeader(String trimmed) {
        if (!isSectionHeader(trimmed)) {
            return null;
        }
        String inner = trimmed.substring(1, trimmed.length() - 1).trim();
        return inner.isEmpty() ? null : inner;
    }

    private static boolean isCommentLine(String trimmedLeft) {
        return trimmedLeft.startsWith("#") || trimmedLeft.startsWith(";");
    }

    private static ParsedGlobalLine parseGlobalLine(String raw) {
        int eq = raw.indexOf('=');
        if (eq < 0) {
            return null;
        }
        String left = raw.substring(0, eq);
        String right = raw.substring(eq + 1);

        String key = left.trim();
        if (key.isEmpty()) {
            return null;
        }

        int rLead = countLeadingSpaces(right);
        int rTrail = countTrailingSpaces(right);
        int end = Math.max(rLead, right.length() - rTrail);

        String valueCore = right.substring(rLead, end);
        String prefix = raw.substring(0, eq + 1) + right.substring(0, rLead);
        String suffix = right.substring(end);

        ParsedGlobalLine gl = new ParsedGlobalLine();
        gl.key = key;
        gl.value = valueCore;
        gl.prefix = prefix;
        gl.suffix = suffix;
        return gl;
    }

    private static ParsedSectionLine parseSectionLine(String raw) {
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        int firstWs = firstWhitespaceIndex(raw);
        ParsedSectionLine sl = new ParsedSectionLine();

        if (firstWs < 0) {
            sl.host = null;
            sl.value = trimmed;
            sl.prefix = "";
            sl.suffix = "";
            return sl;
        }

        String host = raw.substring(0, firstWs);
        String rest = raw.substring(firstWs);

        int lead = countLeadingSpaces(rest);
        int trail = countTrailingSpaces(rest);
        int end = Math.max(lead, rest.length() - trail);

        String valueCore = rest.substring(lead, end);
        String prefix = host + rest.substring(0, lead);
        String suffix = rest.substring(end);

        sl.host = host;
        sl.value = valueCore;
        sl.prefix = prefix;
        sl.suffix = suffix;
        return sl;
    }

    private static int firstWhitespaceIndex(String s) {
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == ' ' || ch == '\t') {
                return i;
            }
        }
        return -1;
    }

    private static int nextIndex(Map<String, Integer> sectionCounters, String section) {
        Integer cur = sectionCounters.get(section);
        int idx = cur == null ? 0 : cur;
        sectionCounters.put(section, idx + 1);
        return idx;
    }

    private static String buildSectionKey(String section, int idx, String host) {
        if (host == null || host.isEmpty()) {
            return section + "[" + idx + "]";
        }
        return section + "[" + idx + "]." + host;
    }

    private static String joinWithLf(List<String> lines) {
        if (lines == null || lines.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            if (i > 0) {
                sb.append('\n');
            }
            sb.append(lines.get(i) == null ? "" : lines.get(i));
        }
        return sb.toString();
    }

    private static String stripLeading(String s) {
        int i = 0;
        while (i < s.length()) {
            char ch = s.charAt(i);
            if (ch != ' ' && ch != '\t') {
                break;
            }
            i++;
        }
        return s.substring(i);
    }

    private static int countLeadingSpaces(String s) {
        int i = 0;
        while (i < s.length()) {
            char ch = s.charAt(i);
            if (ch != ' ' && ch != '\t') {
                break;
            }
            i++;
        }
        return i;
    }

    private static int countTrailingSpaces(String s) {
        int i = s.length() - 1;
        int c = 0;
        while (i >= 0) {
            char ch = s.charAt(i);
            if (ch != ' ' && ch != '\t') {
                break;
            }
            c++;
            i--;
        }
        return c;
    }

    private static String normalizeLf(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        String x = s.replace("\r\n", "\n");
        return x.replace('\r', '\n');
    }

    private static String tokenToLineSep(String token) {
        if (TOKEN_RN.equals(token)) {
            return "\r\n";
        }
        if (TOKEN_R.equals(token)) {
            return "\r";
        }
        return "\n";
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    private static String safeValue(FileDataItem item) {
        Object v = item.getValue();
        return v == null ? "" : v.toString();
    }

    private static String trimFinalLineSep(String s, String ls) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        if (ls == null || ls.isEmpty()) {
            return s;
        }
        if (s.endsWith(ls)) {
            return s.substring(0, s.length() - ls.length());
        }
        return s;
    }


    private static String adjustTrailingLineSeparators(String s, String ls, int count) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        if (ls == null || ls.isEmpty()) {
            return s;
        }
        if (count < 0) {
            count = 0;
        }

        int existing = 0;
        int step = ls.length();
        int i = s.length();
        while (i >= step && s.startsWith(ls, i - step)) {
            existing++;
            i -= step;
        }

        if (existing == count) {
            return s;
        }

        String base = s.substring(0, s.length() - existing * step);
        StringBuilder out = new StringBuilder(base);
        for (int j = 0; j < count; j++) {
            out.append(ls);
        }
        return out.toString();
    }

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString((s == null ? "" : s).getBytes(StandardCharsets.UTF_8));
    }

    private static String unb64(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        byte[] b;
        try {
            b = Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException ex) {
            return "";
        }
        return new String(b, StandardCharsets.UTF_8);
    }

    private static final class ParseContext {
        final LinkedHashMap<String, FileDataItem> out = new LinkedHashMap<>();
        final List<LineEntry> structure = new ArrayList<>();
        final Map<String, Integer> sectionCounters = new HashMap<>();
        final List<String> pendingComments = new ArrayList<>();
        final Set<String> emptySectionCandidates = new HashSet<>();
        final String lineSepToken;
        final boolean endsWithNewline;
        String currentSection;

        ParseContext(ParsedText parsed) {
            this.lineSepToken = parsed == null ? TOKEN_N : parsed.lineSepToken;
            this.endsWithNewline = parsed != null && parsed.endsWithNewline;
        }

        void onEmptyLine() {
            structure.add(LineEntry.empty());
            pendingComments.clear();
        }

        void onSectionHeader(String rawLine, String trimmed) {
            structure.add(LineEntry.section(rawLine));
            currentSection = extractSectionNameFromHeader(trimmed);
            if (currentSection != null) {
                sectionCounters.putIfAbsent(currentSection, 0);
                if (!out.containsKey(currentSection)) {
                    FileDataItem item = new FileDataItem();
                    item.setKey(currentSection);
                    item.setValue("");
                    item.setComment(null);
                    out.put(currentSection, item);
                    emptySectionCandidates.add(currentSection);
                }
            }
            pendingComments.clear();
        }

        void onCommentLine(String rawLine) {
            structure.add(LineEntry.comment(rawLine));
            pendingComments.add(rawLine);
        }

        void onDataOrUnknownLine(String rawLine) {
            if (currentSection == null) {
                handleGlobalLine(rawLine);
            } else {
                handleSectionLine(rawLine);
            }
        }

        void handleGlobalLine(String rawLine) {
            ParsedGlobalLine gl = parseGlobalLine(rawLine);
            if (gl == null) {
                structure.add(LineEntry.comment(rawLine));
                pendingComments.clear();
                return;
            }

            FileDataItem item = new FileDataItem();
            item.setKey(gl.key);
            item.setValue(gl.value);
            attachPendingComment(item);

            out.put(gl.key, item);
            structure.add(LineEntry.data(gl.key, gl.prefix, gl.suffix));
            pendingComments.clear();
        }

        void handleSectionLine(String rawLine) {
            int idx = nextIndex(sectionCounters, currentSection);
            ParsedSectionLine sl = parseSectionLine(rawLine);

            if (sl == null) {
                structure.add(LineEntry.comment(rawLine));
                pendingComments.clear();
                return;
            }

            if (currentSection != null && emptySectionCandidates.contains(currentSection)) {
                out.remove(currentSection);
                emptySectionCandidates.remove(currentSection);
            }

            String key = buildSectionKey(currentSection, idx, sl.host);
            FileDataItem item = new FileDataItem();
            item.setKey(key);
            item.setValue(sl.value);
            attachPendingComment(item);

            out.put(key, item);
            structure.add(LineEntry.data(key, sl.prefix, sl.suffix));
            pendingComments.clear();
        }

        void attachPendingComment(FileDataItem item) {
            if (item == null || pendingComments.isEmpty()) {
                return;
            }
            item.setComment(joinWithLf(pendingComments));
        }

        Map<String, FileDataItem> toResult() {
            MetaState meta = new MetaState();
            meta.lineSepToken = lineSepToken;
            meta.endsWithNewline = endsWithNewline;
            meta.structure = structure;

            FileDataItem metaItem = new FileDataItem();
            metaItem.setKey(META_KEY);
            metaItem.setValue(encodeMeta(meta));
            out.put(META_KEY, metaItem);

            return out;
        }
    }

    private static final class GroupedKeys {
        final List<String> rootKeys;
        final Map<String, List<String>> bySection;

        GroupedKeys(List<String> rootKeys, Map<String, List<String>> bySection) {
            this.rootKeys = rootKeys;
            this.bySection = bySection;
        }
    }

    private static final class ParsedText {
        final List<String> lines;
        final boolean endsWithNewline;
        final String lineSepToken;

        private ParsedText(List<String> lines, boolean endsWithNewline, String lineSepToken) {
            this.lines = lines;
            this.endsWithNewline = endsWithNewline;
            this.lineSepToken = lineSepToken;
        }

        static ParsedText from(String text) {
            String t = text == null ? "" : text;
            String token = detectLineSepToken(t);
            String normalized = normalizeLf(t);
            boolean endsNl = endsWithAnyNewline(t);
            String[] arr = normalized.split("\n", -1);
            int len = arr.length;
            if (len > 0 && arr[len - 1].isEmpty()) {
                String[] trimmed = new String[len - 1];
                System.arraycopy(arr, 0, trimmed, 0, len - 1);
                arr = trimmed;
            }
            List<String> lines = new ArrayList<>();
            Collections.addAll(lines, arr);
            return new ParsedText(lines, endsNl, token);
        }

        private static boolean endsWithAnyNewline(String s) {
            if (s == null || s.isEmpty()) {
                return false;
            }
            char last = s.charAt(s.length() - 1);
            return last == '\n' || last == '\r';
        }

        private static String detectLineSepToken(String s) {
            if (s == null || s.isEmpty()) {
                return TOKEN_N;
            }

            int rn = s.indexOf("\r\n");
            int n = s.indexOf('\n');
            int r = s.indexOf('\r');

            LineSepToken best = bestLineSepToken(rn, n, r);
            return best.token;
        }

        private static LineSepToken bestLineSepToken(int rn, int n, int r) {
            LineSepToken best = new LineSepToken(TOKEN_N, Integer.MAX_VALUE);

            if (rn >= 0) {
                best = best.pick(TOKEN_RN, rn);
            }
            if (r >= 0) {
                best = best.pick(TOKEN_R, r);
            }
            if (n >= 0) {
                best = best.pick(TOKEN_N, n);
            }

            return best;
        }

        private static final class LineSepToken {
            final String token;
            final int index;

            LineSepToken(String token, int index) {
                this.token = token;
                this.index = index;
            }

            LineSepToken pick(String token, int idx) {
                if (idx < index) {
                    return new LineSepToken(token, idx);
                }
                return this;
            }
        }
    }

    private static final class MetaState {
        String lineSepToken;
        boolean endsWithNewline;
        List<LineEntry> structure;
    }

    private enum LineType {
        EMPTY, SECTION, COMMENT, DATA
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

        static LineEntry empty() {
            return new LineEntry(LineType.EMPTY, null, null, null, null);
        }

        static LineEntry section(String text) {
            return new LineEntry(LineType.SECTION, text, null, null, null);
        }

        static LineEntry comment(String text) {
            return new LineEntry(LineType.COMMENT, text, null, null, null);
        }

        static LineEntry data(String key, String prefix, String suffix) {
            return new LineEntry(LineType.DATA, null, key, prefix, suffix);
        }
    }

    private static final class Segment {
        final String section;
        final List<LineEntry> entries;

        Segment(String section, List<LineEntry> entries) {
            this.section = section;
            this.entries = entries;
        }
    }

    private static final class ParsedGlobalLine {
        String key;
        String value;
        String prefix;
        String suffix;
    }

    private static final class ParsedSectionLine {
        String host;
        String value;
        String prefix;
        String suffix;
    }

    private static final class ParsedKey {
        final String section;
        final int index;
        final String host;

        private ParsedKey(String section, int index, String host) {
            this.section = section;
            this.index = index;
            this.host = host;
        }

        static ParsedKey parse(String key) {
            if (key == null) {
                return new ParsedKey(null, 0, null);
            }
            int lb = key.indexOf('[');
            int rb = key.indexOf(']');
            if (lb < 0 || rb < 0 || rb < lb) {
                return new ParsedKey(null, 0, null);
            }
            String sec = key.substring(0, lb);
            String idxStr = key.substring(lb + 1, rb);
            int idx = parseIndex(idxStr);

            String host = parseHost(key, rb);
            return new ParsedKey(sec.isEmpty() ? null : sec, idx, host);
        }

        private static int parseIndex(String idxStr) {
            try {
                return Integer.parseInt(idxStr.trim());
            } catch (NumberFormatException ex) {
                return 0;
            }
        }

        private static String parseHost(String key, int rb) {
            if (rb + 1 >= key.length()) {
                return null;
            }
            if (key.charAt(rb + 1) != '.') {
                return null;
            }
            String host = key.substring(rb + 2);
            return host.isEmpty() ? null : host;
        }
    }
}