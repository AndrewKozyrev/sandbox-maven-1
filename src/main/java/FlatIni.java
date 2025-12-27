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
        String text = data == null ? "" : data;
        ParsedText parsed = ParsedText.from(text);

        LinkedHashMap<String, FileDataItem> out = new LinkedHashMap<>();
        List<LineEntry> structure = new ArrayList<>();

        Map<String, Integer> sectionCounters = new HashMap<>();
        List<String> pendingComments = new ArrayList<>();
        String currentSection = null;

        for (String rawLine : parsed.lines) {
            String trimmed = rawLine.trim();
            if (trimmed.isEmpty()) {
                structure.add(LineEntry.empty());
                pendingComments.clear();
                continue;
            }

            if (isSectionHeader(trimmed)) {
                structure.add(LineEntry.section(rawLine));
                currentSection = extractSectionNameFromHeader(trimmed);
                if (currentSection != null) {
                    sectionCounters.putIfAbsent(currentSection, 0);
                }
                pendingComments.clear();
                continue;
            }

            String leftTrimmed = stripLeading(rawLine);
            if (isCommentLine(leftTrimmed)) {
                structure.add(LineEntry.comment(rawLine));
                pendingComments.add(rawLine);
                continue;
            }

            if (currentSection == null) {
                ParsedGlobalLine gl = parseGlobalLine(rawLine);
                if (gl == null) {
                    structure.add(LineEntry.comment(rawLine));
                    pendingComments.clear();
                    continue;
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
                continue;
            }

            int idx = nextIndex(sectionCounters, currentSection);

            ParsedSectionLine sl = parseSectionLine(rawLine, currentSection);
            if (sl == null) {
                structure.add(LineEntry.comment(rawLine));
                pendingComments.clear();
                continue;
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

        MetaState meta = new MetaState();
        meta.lineSepToken = parsed.lineSepToken;
        meta.endsWithNewline = parsed.endsWithNewline;
        meta.structure = structure;

        FileDataItem metaItem = new FileDataItem();
        metaItem.setKey(META_KEY);
        metaItem.setValue(encodeMeta(meta));
        out.put(META_KEY, metaItem);

        return out;
    }

    @Override
    public String flatToString(Map<String, FileDataItem> data) {
        Map<String, FileDataItem> items = castToItems(data);
        MetaState meta = readMeta(items);
        if (meta == null) {
            return renderWithoutMeta(items);
        }

        String ls = tokenToLineSep(meta.lineSepToken);
        List<LineEntry> structure = meta.structure == null ? Collections.emptyList() : meta.structure;

        Set<String> metaKeys = new HashSet<>();
        for (LineEntry e : structure) {
            if (e.type == LineType.DATA && e.key != null) {
                metaKeys.add(e.key);
            }
        }

        Map<String, List<String>> extrasBySection = computeExtras(items, metaKeys);
        StringBuilder out = new StringBuilder();
        List<Segment> segments = splitSegments(structure);

        Set<String> printedKeys = new HashSet<>();
        for (Segment seg : segments) {
            List<String> extras = seg.sectionName == null ? null : extrasBySection.remove(seg.sectionName);

            boolean originallyHadData = seg.sectionName != null && segmentOriginallyHasData(seg);
            boolean hasAnyDataNow = seg.sectionName != null && segmentHasAnyDataNow(seg, items);
            boolean hasExtras = extras != null && !extras.isEmpty();

            if (originallyHadData && !hasAnyDataNow && !hasExtras) {
                renderOnlyEmptyLines(out, seg, ls);
                continue;
            }

            renderSegment(out, seg, items, printedKeys, extras, ls);
        }

        renderRemainingExtras(out, items, printedKeys, extrasBySection, ls);

        String result = out.toString();
        if (!meta.endsWithNewline) {
            result = trimFinalLineSep(result, ls);
        }
        return result;
    }

    @Override
    public void validate(Map<String, FileDataItem> data) {
        throw new UnsupportedOperationException("Validation of ini/inventory files is not implemented.");
    }

    private static Map<String, FileDataItem> castToItems(Map<?, ?> raw) {
        if (raw == null) {
            return Collections.emptyMap();
        }
        LinkedHashMap<String, FileDataItem> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : raw.entrySet()) {
            Object k = e.getKey();
            if (!(k instanceof String)) {
                continue;
            }
            Object v = e.getValue();
            if (v instanceof FileDataItem) {
                out.put((String) k, (FileDataItem) v);
            }
        }
        return out;
    }

    private static MetaState readMeta(Map<String, FileDataItem> items) {
        FileDataItem metaItem = items.get(META_KEY);
        if (metaItem == null) {
            return null;
        }
        String b64 = metaItem.getComment();
        if (b64 == null || b64.isEmpty()) {
            Object v = metaItem.getValue();
            if (v == null) {
                return null;
            }
            b64 = String.valueOf(v);
        }
        if (b64 == null || b64.isEmpty()) {
            return null;
        }
        return decodeMeta(b64);
    }

    private static String renderWithoutMeta(Map<String, FileDataItem> items) {
        if (items == null || items.isEmpty()) {
            return "";
        }

        String ls = System.lineSeparator();
        StringBuilder sb = new StringBuilder();

        List<String> keys = new ArrayList<>();
        for (String k : items.keySet()) {
            if (k == null || META_KEY.equals(k)) {
                continue;
            }
            keys.add(k);
        }
        keys.sort(String::compareTo);

        Map<String, List<String>> bySection = new LinkedHashMap<>();
        List<String> rootKeys = new ArrayList<>();

        for (String k : keys) {
            ParsedKey pk = ParsedKey.parse(k);
            if (pk.section == null) {
                rootKeys.add(k);
            } else {
                bySection.computeIfAbsent(pk.section, x -> new ArrayList<>()).add(k);
            }
        }

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

        List<LineEntry> entries = seg.entries == null ? Collections.emptyList() : seg.entries;
        int trailingEmpty = countTrailingEmpty(entries);
        int limit = entries.size() - trailingEmpty;

        List<String> pendingComments = new ArrayList<>();

        for (int i = 0; i < limit; i++) {
            LineEntry e = entries.get(i);
            if (e.type == LineType.COMMENT) {
                pendingComments.add(e.text == null ? "" : e.text);
                continue;
            }
            if (e.type == LineType.EMPTY) {
                flushRawComments(out, pendingComments, ls);
                out.append(ls);
                pendingComments.clear();
                continue;
            }
            if (e.type == LineType.SECTION) {
                flushRawComments(out, pendingComments, ls);
                out.append(e.text == null ? "" : e.text).append(ls);
                pendingComments.clear();
                continue;
            }
            if (e.type == LineType.DATA) {
                String key = e.key;
                FileDataItem item = key == null ? null : items.get(key);

                if (item == null || META_KEY.equals(key)) {
                    flushRawComments(out, pendingComments, ls);
                    pendingComments.clear();
                    continue;
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
        }

        flushRawComments(out, pendingComments, ls);
        pendingComments.clear();

        if (extras != null && !extras.isEmpty()) {
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

        out.append(String.valueOf(ls).repeat(Math.max(0, trailingEmpty)));
    }

    private static void renderRemainingExtras(StringBuilder out,
                                              Map<String, FileDataItem> items,
                                              Set<String> printedKeys,
                                              Map<String, List<String>> extrasBySection,
                                              String ls) {

        List<String> globalExtras = extrasBySection.remove(null);
        if (globalExtras != null) {
            globalExtras.sort(String::compareTo);
            for (String k : globalExtras) {
                FileDataItem item = items.get(k);
                if (item == null || META_KEY.equals(k)) {
                    continue;
                }
                appendCommentBlock(out, item.getComment(), ls);
                out.append(k).append('=').append(safeValue(item)).append(ls);
                printedKeys.add(k);
            }
        }

        List<String> sectionNames = new ArrayList<>(extrasBySection.keySet());
        sectionNames.sort(String::compareTo);

        for (String sec : sectionNames) {
            List<String> keys = extrasBySection.get(sec);
            if (keys == null || keys.isEmpty()) {
                continue;
            }
            if (out.length() > 0) {
                out.append(ls);
            }
            out.append('[').append(sec).append(']').append(ls);

            keys.sort(Comparator.comparingInt(a -> ParsedKey.parse(a).index));
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
            String sec = pk.section;
            bySection.computeIfAbsent(sec, x -> new ArrayList<>()).add(k);
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
        if (seg == null || seg.entries == null) {
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
            if (entries.get(i).type == LineType.EMPTY) {
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
                }
            }
        }
        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static MetaState decodeMeta(String metaB64) {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(metaB64);
        } catch (IllegalArgumentException ex) {
            return null;
        }
        String payload = new String(bytes, StandardCharsets.UTF_8);
        String norm = normalizeLf(payload);
        String[] lines = norm.split("\n", -1);
        if (lines.length < 4) {
            return null;
        }
        if (!META_VERSION.equals(lines[0])) {
            return null;
        }

        MetaState meta = new MetaState();
        meta.lineSepToken = lines[1] == null || lines[1].isEmpty() ? TOKEN_N : lines[1];
        meta.endsWithNewline = "1".equals(lines[2]);

        int count;
        try {
            count = Integer.parseInt(lines[3].trim());
        } catch (NumberFormatException ex) {
            return null;
        }

        List<LineEntry> structure = new ArrayList<>();
        int idx = 4;
        while (idx < lines.length && structure.size() < count) {
            String line = lines[idx++];
            if (line.isEmpty()) {
                continue;
            }
            char c = line.charAt(0);
            if (c == 'E') {
                structure.add(LineEntry.empty());
                continue;
            }
            int t = line.indexOf('\t');
            String p = t < 0 ? "" : line.substring(t + 1);
            if (c == 'S') {
                structure.add(LineEntry.section(unb64(p)));
            } else if (c == 'C') {
                structure.add(LineEntry.comment(unb64(p)));
            } else if (c == 'D') {
                String[] parts = splitTabs4(line);
                String key = unb64(parts[1]);
                String prefix = unb64(parts[2]);
                String suffix = unb64(parts[3]);
                structure.add(LineEntry.data(key, prefix, suffix));
            }
        }

        meta.structure = structure;
        return meta;
    }

    private static String[] splitTabs4(String line) {
        String[] out = new String[4];
        int start = 0;
        for (int i = 0; i < 3; i++) {
            int t = line.indexOf('\t', start);
            if (t < 0) {
                out[i] = line.substring(start);
                for (int j = i + 1; j < 4; j++) {
                    out[j] = "";
                }
                for (int k = 0; k < out.length; k++) {
                    if (out[k] == null) {
                        out[k] = "";
                    }
                }
                return out;
            }
            out[i] = line.substring(start, t);
            start = t + 1;
        }
        out[3] = start <= line.length() ? line.substring(start) : "";
        for (int i = 0; i < out.length; i++) {
            if (out[i] == null) {
                out[i] = "";
            }
        }
        return out;
    }

    private static String b64(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String unb64(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        try {
            return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            return "";
        }
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
        if (ls != null && !ls.isEmpty() && s.endsWith(ls)) {
            return s.substring(0, s.length() - ls.length());
        }
        return s;
    }

    private static boolean isSectionHeader(String trimmed) {
        return trimmed.startsWith("[") && trimmed.endsWith("]") && trimmed.length() >= 3;
    }

    private static String extractSectionNameFromHeader(String trimmed) {
        if (!isSectionHeader(trimmed)) {
            return null;
        }
        String inside = trimmed.substring(1, trimmed.length() - 1).trim();
        return inside.isEmpty() ? null : inside;
    }

    private static boolean isCommentLine(String leftTrimmed) {
        return leftTrimmed.startsWith("#") || leftTrimmed.startsWith(";");
    }

    private static String stripLeading(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        int i = 0;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return s.substring(i);
    }

    private static String normalizeLf(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        return s.replace("\r\n", "\n").replace('\r', '\n');
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

    private static String nvl(String s) {
        return s == null ? "" : s;
    }

    private static String safeValue(FileDataItem item) {
        Object v = item == null ? null : item.getValue();
        return v == null ? "" : String.valueOf(v);
    }

    private static int nextIndex(Map<String, Integer> counters, String section) {
        Integer v = counters.get(section);
        int idx = v == null ? 0 : v;
        counters.put(section, idx + 1);
        return idx;
    }

    private static String buildSectionKey(String section, int idx, String host) {
        String base = section + "[" + idx + "]";
        if (host == null || host.isEmpty()) {
            return base;
        }
        return base + "." + host;
    }

    private static boolean isChildrenSection(String section) {
        if (section == null) {
            return false;
        }
        return section.trim().toLowerCase().endsWith(":children");
    }

    private static ParsedSectionLine parseSectionLine(String rawLine, String sectionName) {
        if (rawLine == null) {
            return null;
        }
        if (isChildrenSection(sectionName)) {
            return parseValueOnlyLine(rawLine);
        }

        int first = firstNonWs(rawLine, 0);
        if (first < 0) {
            return null;
        }
        int tokenEnd = firstWs(rawLine, first);
        if (tokenEnd < 0) {
            return parseValueOnlyLine(rawLine);
        }

        String host = rawLine.substring(first, tokenEnd);
        int restStart = firstNonWs(rawLine, tokenEnd);
        if (restStart < 0) {
            restStart = rawLine.length();
        }
        String prefix = rawLine.substring(0, restStart);
        String rest = rawLine.substring(restStart);
        String restNoTrail = stripTrailing(rest);
        String suffix = rest.substring(restNoTrail.length());
        return new ParsedSectionLine(host, restNoTrail, prefix, suffix);
    }

    private static ParsedSectionLine parseValueOnlyLine(String rawLine) {
        int lead = countLeadingWs(rawLine);
        int trail = countTrailingWs(rawLine);
        String prefix = rawLine.substring(0, lead);
        String suffix = rawLine.substring(rawLine.length() - trail);
        String value = rawLine.trim();
        return new ParsedSectionLine(null, value, prefix, suffix);
    }

    private static ParsedGlobalLine parseGlobalLine(String rawLine) {
        int eq = rawLine.indexOf('=');
        if (eq < 0) {
            return null;
        }
        String left = rawLine.substring(0, eq);
        String key = left.trim();
        if (key.isEmpty()) {
            return null;
        }
        int after = eq + 1;
        while (after < rawLine.length() && Character.isWhitespace(rawLine.charAt(after))) {
            after++;
        }
        String prefix = rawLine.substring(0, after);
        String rest = rawLine.substring(after);
        String restNoTrail = stripTrailing(rest);
        String suffix = rest.substring(restNoTrail.length());
        return new ParsedGlobalLine(key, restNoTrail, prefix, suffix);
    }

    private static String stripTrailing(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        int end = s.length();
        while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(0, end);
    }

    private static int countLeadingWs(String s) {
        int i = 0;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }

    private static int countTrailingWs(String s) {
        int i = s.length();
        while (i > 0 && Character.isWhitespace(s.charAt(i - 1))) {
            i--;
        }
        return s.length() - i;
    }

    private static int firstNonWs(String s, int from) {
        int i = from;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i >= s.length() ? -1 : i;
    }

    private static int firstWs(String s, int from) {
        for (int i = from; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static final class ParsedText {
        final List<String> lines;
        final String lineSepToken;
        final boolean endsWithNewline;

        private ParsedText(List<String> lines, String lineSepToken, boolean endsWithNewline) {
            this.lines = lines;
            this.lineSepToken = lineSepToken;
            this.endsWithNewline = endsWithNewline;
        }

        static ParsedText from(String data) {
            String token = detectLineSepToken(data);
            boolean ends = data.endsWith("\n") || data.endsWith("\r");

            String[] raw = data.split("\\R", -1);
            List<String> lines = new ArrayList<>(raw.length);
            Collections.addAll(lines, raw);

            if (ends && !lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
                lines.remove(lines.size() - 1);
            }

            return new ParsedText(lines, token, ends);
        }

        private static String detectLineSepToken(String data) {
            int idx = data.indexOf("\r\n");
            if (idx >= 0) {
                return TOKEN_RN;
            }
            idx = data.indexOf('\n');
            if (idx >= 0) {
                return TOKEN_N;
            }
            idx = data.indexOf('\r');
            if (idx >= 0) {
                return TOKEN_R;
            }
            return TOKEN_N;
        }
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

        static LineEntry section(String rawLine) {
            return new LineEntry(LineType.SECTION, rawLine, null, null, null);
        }

        static LineEntry comment(String rawLine) {
            return new LineEntry(LineType.COMMENT, rawLine, null, null, null);
        }

        static LineEntry data(String key, String prefix, String suffix) {
            return new LineEntry(LineType.DATA, null, key, prefix == null ? "" : prefix, suffix == null ? "" : suffix);
        }
    }

    private static final class MetaState {
        String lineSepToken;
        boolean endsWithNewline;
        List<LineEntry> structure;
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
            if (key == null || key.isEmpty()) {
                return new ParsedKey(null, 0, null);
            }

            int lb = key.indexOf('[');
            int rb = lb < 0 ? -1 : key.indexOf(']', lb + 1);
            if (lb < 0 || rb < 0) {
                return new ParsedKey(null, 0, null);
            }

            String section = lb == 0 ? null : key.substring(0, lb);
            String idxStr = key.substring(lb + 1, rb).trim();
            int idx = 0;
            if (!idxStr.isEmpty()) {
                try {
                    idx = Integer.parseInt(idxStr);
                } catch (NumberFormatException ignored) {
                }
            }

            String host = null;
            if (rb + 1 < key.length() && key.charAt(rb + 1) == '.') {
                host = key.substring(rb + 2);
                if (host.isEmpty()) {
                    host = null;
                }
            }

            return new ParsedKey(section, idx, host);
        }
    }

    private static final class ParsedSectionLine {
        final String host;
        final String value;
        final String prefix;
        final String suffix;

        private ParsedSectionLine(String host, String value, String prefix, String suffix) {
            this.host = host;
            this.value = value == null ? "" : value;
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
        }
    }

    private static final class ParsedGlobalLine {
        final String key;
        final String value;
        final String prefix;
        final String suffix;

        private ParsedGlobalLine(String key, String value, String prefix, String suffix) {
            this.key = key;
            this.value = value == null ? "" : value;
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
        }
    }

    private static final class Segment {
        final String sectionName;
        final List<LineEntry> entries;

        private Segment(String sectionName, List<LineEntry> entries) {
            this.sectionName = sectionName;
            this.entries = entries == null ? Collections.emptyList() : entries;
        }
    }
}