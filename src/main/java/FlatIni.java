import org.apache.commons.lang3.NotImplementedException;

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
            ctx.accept(rawLine);
        }
        ctx.finish();
        return ctx.out;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        MetaState meta = readMeta(data);
        if (meta == null || meta.structure == null || meta.structure.isEmpty()) {
            return dumpFallback(data, System.lineSeparator());
        }

        String ls = tokenToLineSep(meta.lineSepToken);
        String out = dumpWithStructure(data, meta, ls);
        if (!meta.endsWithNewline) {
            out = trimFinalLineSep(out, ls);
        }
        return out;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new NotImplementedException("Validation of .ini files is not implemented yet.");
    }

    private static final class ParseContext {
        final LinkedHashMap<String, FileDataItem> out = new LinkedHashMap<>();
        final List<LineEntry> structure = new ArrayList<>();
        final Map<String, Integer> sectionCounters = new HashMap<>();
        final List<String> pendingComments = new ArrayList<>();
        final ParsedText parsed;
        String currentSection;

        ParseContext(ParsedText parsed) {
            this.parsed = parsed;
        }

        void accept(String rawLine) {
            String line = rawLine == null ? "" : rawLine;
            String trimmed = line.trim();

            if (trimmed.isEmpty()) {
                onEmpty();
                return;
            }

            if (isSectionHeader(trimmed)) {
                onSection(line, trimmed);
                return;
            }

            String leftTrimmed = stripLeading(line);
            if (isCommentLine(leftTrimmed)) {
                onComment(line);
                return;
            }

            if (currentSection == null) {
                onGlobal(line);
                return;
            }

            onSectionValue(line);
        }

        void finish() {
            MetaState meta = new MetaState();
            meta.lineSepToken = parsed.lineSepToken;
            meta.endsWithNewline = parsed.endsWithNewline;
            meta.structure = structure;

            FileDataItem metaItem = new FileDataItem();
            metaItem.setKey(META_KEY);
            metaItem.setValue(encodeMeta(meta));
            out.put(META_KEY, metaItem);
        }

        private void onEmpty() {
            structure.add(LineEntry.empty());
            pendingComments.clear();
        }

        private void onSection(String rawLine, String trimmed) {
            structure.add(LineEntry.section(rawLine));
            currentSection = extractSectionNameFromHeader(trimmed);
            if (currentSection != null) {
                sectionCounters.putIfAbsent(currentSection, 0);
            }
            pendingComments.clear();
        }

        private void onComment(String rawLine) {
            structure.add(LineEntry.comment(rawLine));
            pendingComments.add(rawLine);
        }

        private void onGlobal(String rawLine) {
            ParsedGlobalLine gl = parseGlobalLine(rawLine);
            if (gl == null) {
                structure.add(LineEntry.comment(rawLine));
                pendingComments.clear();
                return;
            }

            FileDataItem item = new FileDataItem();
            item.setKey(gl.key);
            item.setValue(gl.value);
            if (!pendingComments.isEmpty()) {
                item.setComment(joinWithLf(pendingComments));
            }
            out.put(gl.key, item);
            structure.add(LineEntry.data(gl.key, gl.prefix, gl.suffix));
            pendingComments.clear();
        }

        private void onSectionValue(String rawLine) {
            int idx = nextIndex(sectionCounters, currentSection);
            ParsedSectionLine sl = parseSectionLine(rawLine);
            if (sl == null) {
                structure.add(LineEntry.comment(rawLine));
                pendingComments.clear();
                return;
            }

            String key = buildSectionKey(currentSection, idx, sl.host);
            FileDataItem item = new FileDataItem();
            item.setKey(key);
            item.setValue(sl.value);
            if (!pendingComments.isEmpty()) {
                item.setComment(joinWithLf(pendingComments));
            }
            out.put(key, item);
            structure.add(LineEntry.data(key, sl.prefix, sl.suffix));
            pendingComments.clear();
        }
    }

    private static String dumpWithStructure(Map<String, FileDataItem> items, MetaState meta, String ls) {
        DumpContext ctx = new DumpContext(items, meta, ls);
        return ctx.dump();
    }

    private static final class DumpContext {
        final Map<String, FileDataItem> items;
        final MetaState meta;
        final String ls;
        final Set<String> metaKeys;
        final Map<String, List<String>> extrasBySection;
        final Set<String> printedKeys = new HashSet<>();

        DumpContext(Map<String, FileDataItem> items, MetaState meta, String ls) {
            this.items = items;
            this.meta = meta;
            this.ls = ls;
            this.metaKeys = collectMetaKeys(meta);
            this.extrasBySection = computeExtras(items, metaKeys);
        }

        String dump() {
            List<Segment> segments = splitSegments(meta.structure);
            StringBuilder out = new StringBuilder();

            for (Segment seg : segments) {
                List<String> segExtras = seg.section == null
                        ? extrasBySection.getOrDefault(null, Collections.emptyList())
                        : extrasBySection.getOrDefault(seg.section, Collections.emptyList());

                boolean originallyHadData = segmentOriginallyHasData(seg);
                boolean hasAnyNow = segmentHasAnyDataNow(seg, items);
                boolean hasExtras = segExtras != null && !segExtras.isEmpty();

                if (originallyHadData && !hasAnyNow && !hasExtras) {
                    renderOnlyEmptyLines(out, seg, ls);
                } else {
                    renderSegment(out, seg, items, printedKeys, segExtras, ls);
                }
            }

            appendRemainingExtras(out, items, printedKeys, metaKeys, ls);
            return out.toString();
        }

        private static Set<String> collectMetaKeys(MetaState meta) {
            Set<String> keys = new HashSet<>();
            if (meta == null || meta.structure == null) {
                return keys;
            }
            for (LineEntry e : meta.structure) {
                if (e != null && e.type == LineType.DATA && e.key != null) {
                    keys.add(e.key);
                }
            }
            return keys;
        }
    }

    private static void appendRemainingExtras(StringBuilder out,
                                              Map<String, FileDataItem> items,
                                              Set<String> printedKeys,
                                              Set<String> metaKeys,
                                              String ls) {
        Map<String, List<String>> bySection = new HashMap<>();
        List<String> rootKeys = new ArrayList<>();

        for (String k : items.keySet()) {
            if (k == null || META_KEY.equals(k) || metaKeys.contains(k) || printedKeys.contains(k)) {
                continue;
            }
            ParsedKey pk = ParsedKey.parse(k);
            if (pk.section == null) {
                rootKeys.add(k);
            } else {
                bySection.computeIfAbsent(pk.section, x -> new ArrayList<>()).add(k);
            }
        }

        rootKeys.sort(String::compareTo);

        for (String k : rootKeys) {
            FileDataItem item = items.get(k);
            if (item == null) {
                continue;
            }
            appendCommentBlock(out, item.getComment(), ls);
            out.append(k).append('=').append(safeValue(item)).append(ls);
            printedKeys.add(k);
        }

        List<String> sectionNames = new ArrayList<>(bySection.keySet());
        sectionNames.sort(String::compareTo);

        for (String sec : sectionNames) {
            if (out.length() > 0) {
                out.append(ls);
            }
            out.append('[').append(sec).append(']').append(ls);

            List<String> keys = bySection.get(sec);
            keys.sort((a, b) -> {
                ParsedKey pa = ParsedKey.parse(a);
                ParsedKey pb = ParsedKey.parse(b);
                int c = Integer.compare(pa.index, pb.index);
                if (c != 0) {
                    return c;
                }
                String ha = pa.host == null ? "" : pa.host;
                String hb = pb.host == null ? "" : pb.host;
                return ha.compareTo(hb);
            });

            for (String k : keys) {
                FileDataItem item = items.get(k);
                if (item == null || META_KEY.equals(k)) {
                    continue;
                }
                appendCommentBlock(out, item.getComment(), ls);
                ParsedKey pk = ParsedKey.parse(k);
                String v = safeValue(item);
                if (pk.host == null) {
                    out.append(v).append(ls);
                } else {
                    out.append(pk.host);
                    if (!v.isEmpty()) {
                        out.append(' ').append(v);
                    }
                    out.append(ls);
                }
                printedKeys.add(k);
            }
        }
    }

    private static String dumpFallback(Map<String, FileDataItem> items, String ls) {
        Map<String, List<String>> bySection = new HashMap<>();
        List<String> rootKeys = new ArrayList<>();

        for (String k : items.keySet()) {
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            ParsedKey pk = ParsedKey.parse(k);
            if (pk.section == null) {
                rootKeys.add(k);
            } else {
                bySection.computeIfAbsent(pk.section, x -> new ArrayList<>()).add(k);
            }
        }

        rootKeys.sort(String::compareTo);

        StringBuilder sb = new StringBuilder();

        for (String k : rootKeys) {
            FileDataItem item = items.get(k);
            if (item == null) {
                continue;
            }
            appendCommentBlock(sb, item.getComment(), ls);
            sb.append(k).append('=').append(safeValue(item)).append(ls);
        }

        List<String> sectionNames = new ArrayList<>(bySection.keySet());
        sectionNames.sort(String::compareTo);

        for (String section : sectionNames) {
            if (sb.length() > 0) {
                sb.append(ls);
            }
            sb.append('[').append(section).append(']').append(ls);

            List<String> list = bySection.get(section);
            list.sort((a, b) -> {
                ParsedKey pa = ParsedKey.parse(a);
                ParsedKey pb = ParsedKey.parse(b);
                int c = Integer.compare(pa.index, pb.index);
                if (c != 0) {
                    return c;
                }
                String ha = pa.host == null ? "" : pa.host;
                String hb = pb.host == null ? "" : pb.host;
                return ha.compareTo(hb);
            });

            for (String k : list) {
                FileDataItem item = items.get(k);
                if (item == null) {
                    continue;
                }
                appendCommentBlock(sb, item.getComment(), ls);
                ParsedKey pk = ParsedKey.parse(k);
                String v = safeValue(item);
                if (pk.host == null) {
                    sb.append(v).append(ls);
                } else {
                    sb.append(pk.host);
                    if (!v.isEmpty()) {
                        sb.append(' ').append(v);
                    }
                    sb.append(ls);
                }
            }
        }

        return trimFinalLineSep(sb.toString(), ls);
    }

    private static void renderSegment(StringBuilder out,
                                      Segment seg,
                                      Map<String, FileDataItem> items,
                                      Set<String> printedKeys,
                                      List<String> extras,
                                      String ls) {
        SegmentRenderer renderer = new SegmentRenderer(out, seg, items, printedKeys, extras, ls);
        renderer.render();
    }

    private static final class SegmentRenderer {
        final StringBuilder out;
        final Segment seg;
        final Map<String, FileDataItem> items;
        final Set<String> printedKeys;
        final List<String> extras;
        final String ls;
        final List<String> pendingComments = new ArrayList<>();
        boolean skipEmptyAfterMissing;

        SegmentRenderer(StringBuilder out,
                        Segment seg,
                        Map<String, FileDataItem> items,
                        Set<String> printedKeys,
                        List<String> extras,
                        String ls) {
            this.out = out;
            this.seg = seg;
            this.items = items;
            this.printedKeys = printedKeys;
            this.extras = extras;
            this.ls = ls;
        }

        void render() {
            List<LineEntry> entries = seg.entries == null ? Collections.emptyList() : seg.entries;
            int trailingEmpty = countTrailingEmpty(entries);
            int limit = entries.size() - trailingEmpty;

            for (int i = 0; i < limit; i++) {
                LineEntry e = entries.get(i);
                if (e != null) {
                    handleEntry(e);
                }
            }

            flushRawComments(out, pendingComments, ls);
            pendingComments.clear();

            renderExtras();

            if (trailingEmpty > 0) {
                out.append(String.valueOf(ls).repeat(trailingEmpty));
            }
        }

        private void handleEntry(LineEntry e) {
            if (e.type != LineType.EMPTY) {
                skipEmptyAfterMissing = false;
            }
            if (e.type == LineType.COMMENT) {
                pendingComments.add(e.text == null ? "" : e.text);
                return;
            }
            if (e.type == LineType.SECTION) {
                flushRawComments(out, pendingComments, ls);
                pendingComments.clear();
                out.append(e.text == null ? "" : e.text).append(ls);
                return;
            }
            if (e.type == LineType.EMPTY) {
                if (skipEmptyAfterMissing) {
                    return;
                }
                flushRawComments(out, pendingComments, ls);
                pendingComments.clear();
                out.append(ls);
                return;
            }
            if (e.type == LineType.DATA) {
                renderData(e);
            }
        }

        private void renderData(LineEntry e) {
            String key = e.key;
            FileDataItem item = key == null ? null : items.get(key);

            if (item == null || META_KEY.equals(key)) {
                pendingComments.clear();
                skipEmptyAfterMissing = true;
                return;
            }

            String originalBlock = joinWithLf(pendingComments);
            String newComment = item.getComment();

            if (newComment != null && !newComment.isEmpty()) {
                if (newComment.equals(originalBlock)) {
                    flushRawComments(out, pendingComments, ls);
                } else {
                    appendCommentBlock(out, newComment, ls);
                }
            }

            pendingComments.clear();
            printedKeys.add(key);

            String value = safeValue(item);
            out.append(nvl(e.prefix)).append(value).append(nvl(e.suffix)).append(ls);
        }

        private void renderExtras() {
            if (extras == null || extras.isEmpty()) {
                return;
            }
            extras.sort(Comparator.comparingInt(a -> ParsedKey.parse(a).index));
            for (String k : extras) {
                FileDataItem item = items.get(k);
                if (item == null || META_KEY.equals(k)) {
                    continue;
                }
                appendCommentBlock(out, item.getComment(), ls);
                ParsedKey pk = ParsedKey.parse(k);
                String v = safeValue(item);
                if (pk.host == null) {
                    out.append(v).append(ls);
                } else {
                    out.append(pk.host);
                    if (!v.isEmpty()) {
                        out.append(' ').append(v);
                    }
                    out.append(ls);
                }
                printedKeys.add(k);
            }
        }
    }

    private static Map<String, List<String>> computeExtras(Map<String, FileDataItem> items, Set<String> metaKeys) {
        Map<String, List<String>> bySection = new HashMap<>();
        for (String k : items.keySet()) {
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            if (metaKeys.contains(k)) {
                continue;
            }
            ParsedKey pk = ParsedKey.parse(k);
            bySection.computeIfAbsent(pk.section, x -> new ArrayList<>()).add(k);
        }
        return bySection;
    }

    private static boolean segmentOriginallyHasData(Segment seg) {
        if (seg.entries == null) {
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
        if (seg.entries == null) {
            return false;
        }
        for (LineEntry e : seg.entries) {
            if (e == null || e.type != LineType.DATA) {
                continue;
            }
            String k = e.key;
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            if (items.get(k) != null) {
                return true;
            }
        }
        return false;
    }

    private static void renderOnlyEmptyLines(StringBuilder out, Segment seg, String ls) {
        if (seg.entries == null) {
            return;
        }
        for (LineEntry e : seg.entries) {
            if (e != null && e.type == LineType.EMPTY) {
                out.append(ls);
            }
        }
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

    private static void flushRawComments(StringBuilder out, List<String> pending, String ls) {
        if (pending == null || pending.isEmpty()) {
            return;
        }
        for (String line : pending) {
            out.append(line == null ? "" : line).append(ls);
        }
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
                continue;
            }
            String t = stripLeading(p);
            if (t.startsWith("#") || t.startsWith(";")) {
                out.append(p).append(ls);
            } else {
                out.append('#').append(p).append(ls);
            }
        }
    }

    private static String encodeMeta(MetaState meta) {
        StringBuilder sb = new StringBuilder();
        sb.append(META_VERSION).append('\n');
        sb.append(meta.lineSepToken == null ? TOKEN_N : meta.lineSepToken).append('\n');
        sb.append(meta.endsWithNewline ? '1' : '0').append('\n');
        int n = meta.structure == null ? 0 : meta.structure.size();
        sb.append(n).append('\n');
        if (n > 0) {
            for (LineEntry e : meta.structure) {
                if (e == null) {
                    sb.append('E').append('\n');
                    continue;
                }
                if (e.type == LineType.EMPTY) {
                    sb.append('E').append('\n');
                } else if (e.type == LineType.SECTION) {
                    sb.append('S').append('\t').append(b64(nvl(e.text))).append('\n');
                } else if (e.type == LineType.COMMENT) {
                    sb.append('C').append('\t').append(b64(nvl(e.text))).append('\n');
                } else if (e.type == LineType.DATA) {
                    sb.append('D').append('\t')
                            .append(b64(nvl(e.key))).append('\t')
                            .append(b64(nvl(e.prefix))).append('\t')
                            .append(b64(nvl(e.suffix))).append('\n');
                } else {
                    sb.append('E').append('\n');
                }
            }
        }
        return b64(sb.toString());
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
        String s = String.valueOf(v);
        if (s.isEmpty()) {
            return null;
        }
        try {
            String decoded = ub64(s);
            return decodeMeta(decoded);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static MetaState decodeMeta(String decoded) {
        if (decoded == null || decoded.isEmpty()) {
            return null;
        }
        String norm = normalizeLf(decoded);
        String[] lines = norm.split("\n", -1);
        if (lines.length < 4) {
            return null;
        }
        if (!META_VERSION.equals(lines[0])) {
            return null;
        }

        String token = lines[1];
        boolean endsWithNewline = "1".equals(lines[2]);

        int n;
        try {
            n = Integer.parseInt(lines[3].trim());
        } catch (NumberFormatException e) {
            return null;
        }
        int expected = 4 + n;
        if (n < 0 || lines.length < expected) {
            return null;
        }

        List<LineEntry> structure = new ArrayList<>();
        int idx = 4;
        for (int i = 0; i < n; i++) {
            String row = lines[idx++];
            if (row == null || row.isEmpty()) {
                structure.add(LineEntry.empty());
                continue;
            }
            char kind = row.charAt(0);
            if (kind == 'E') {
                structure.add(LineEntry.empty());
                continue;
            }
            if (kind == 'S') {
                String[] parts = row.split("\t", -1);
                String text = parts.length > 1 ? ub64(parts[1]) : "";
                structure.add(LineEntry.section(text));
                continue;
            }
            if (kind == 'C') {
                String[] parts = row.split("\t", -1);
                String text = parts.length > 1 ? ub64(parts[1]) : "";
                structure.add(LineEntry.comment(text));
                continue;
            }
            if (kind == 'D') {
                String[] parts = row.split("\t", -1);
                String key = parts.length > 1 ? ub64(parts[1]) : "";
                String prefix = parts.length > 2 ? ub64(parts[2]) : "";
                String suffix = parts.length > 3 ? ub64(parts[3]) : "";
                structure.add(LineEntry.data(key, prefix, suffix));
                continue;
            }
            structure.add(LineEntry.empty());
        }

        MetaState meta = new MetaState();
        meta.lineSepToken = token;
        meta.endsWithNewline = endsWithNewline;
        meta.structure = structure;
        return meta;
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

    private static String safeValue(FileDataItem item) {
        Object v = item.getValue();
        return v == null ? "" : String.valueOf(v);
    }

    private static String normalizeLf(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        String t = s.replace("\r\n", "\n");
        return t.replace('\r', '\n');
    }

    private static String b64(String s) {
        byte[] bytes = s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static String ub64(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        byte[] decoded = Base64.getDecoder().decode(s);
        return new String(decoded, StandardCharsets.UTF_8);
    }

    private static String nvl(String s) {
        return s == null ? "" : s;
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

            int min = Integer.MAX_VALUE;
            String token = TOKEN_N;

            if (rn >= 0) {
                min = rn;
                token = TOKEN_RN;
            }
            if (r >= 0 && r < min) {
                min = r;
                token = TOKEN_R;
            }
            if (n >= 0 && n < min) {
                token = TOKEN_N;
            }
            return token;
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
            int idx = 0;
            try {
                idx = Integer.parseInt(idxStr.trim());
            } catch (NumberFormatException ignored) {
            }
            String host = null;
            if (rb + 1 < key.length() && key.charAt(rb + 1) == '.') {
                host = key.substring(rb + 2);
                if (host.isEmpty()) {
                    host = null;
                }
            }
            return new ParsedKey(sec.isEmpty() ? null : sec, idx, host);
        }
    }
}