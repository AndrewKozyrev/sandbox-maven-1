import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FlatIniTest {

    private static final String ORIGINAL_1_INI = "src/test/resources/flat_mapper/ini/original_1.ini";
    private static final String FLAT_ORIGINAL_1 = "src/test/resources/flat_mapper/ini/flat_original_1.txt";
    private static final String RECONSTRUCTED_ORIGINAL_1 = "src/test/resources/flat_mapper/ini/reconstructed_original.ini";
    private static final String INPUT_2 = "src/test/resources/flat_mapper/ini/input_2.ini";
    private static final String INPUT_3 = "src/test/resources/flat_mapper/ini/input_3.ini";
    private static final String EXPECTED_3 = "src/test/resources/flat_mapper/ini/expected_3.txt";
    private static final String EXPECTED_4 = "src/test/resources/flat_mapper/ini/expected_4.ini";

    private final FlatIni flatIni = new FlatIni();

    @Test
    void flatToMap_flattensCorrectly() throws Exception {
        var inputData = Files.readString(Paths.get(ORIGINAL_1_INI));
        var items = flatIni.flatToMap(inputData);
        var expectedData = Files.readAllLines(Paths.get(FLAT_ORIGINAL_1))
                .stream()
                .map(x -> x.split("\\s*=\\s*", 2))
                .collect(Collectors.toMap(x -> x[0], y -> y.length > 1 ? y[1] : ""));
        for (String key : items.keySet()) {
            var expectedValue = expectedData.get(key);
            var actualValue = items.get(key).getValue().toString();
            assertEquals(expectedValue, actualValue);
        }
    }

    @Test
    void flatToString_unflattensCorrectly() throws Exception {
        var inputData = Files.readString(Paths.get(ORIGINAL_1_INI));
        var items = flatIni.flatToMap(inputData);
        var actual = flatIni.flatToString(items);
        var expected = Files.readString(Paths.get(RECONSTRUCTED_ORIGINAL_1));
        assertEquals(expected, actual);
    }

    @Test
    void flatToMap_singleParameter() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_2));
        var items = flatIni.flatToMap(inputData);
        assertNotNull(items.get("param1"));
        assertEquals("value1", items.get("param1").getValue());
    }

    @Test
    void flatToMap_correctOrderOfParams() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_3));
        var items = flatIni.flatToMap(inputData);
        var actual = new ArrayList<>(items.values());
        var expected = Files.readAllLines(Paths.get(EXPECTED_3))
                .stream()
                .map(x -> x.split("\\s*=\\s*", 2))
                .map(x -> new SimpleEntry<>(x[0], x[1]))
                .collect(Collectors.toList());
        for (int i = 0; i < actual.size(); i++) {
            var expectedKey = expected.get(i).getKey();
            var actualKey = actual.get(i).getKey();
            assertEquals(expectedKey, actualKey);
            var expectedValue = expected.get(i).getValue();
            var actualValue = actual.get(i).getValue();
            assertEquals(expectedValue, actualValue);
        }
    }

    @Test
    void flatToMap_commentsMapping() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_3));
        var map = flatIni.flatToMap(inputData);
        assertEquals("Имя кластера", map.get("all:vars[0]").getComment());
        assertEquals("хост под Sberl & PES", map.get("blue[0].tslds-efs002596.ufsflcore.delta.sbrf.ru").getComment());
        assertEquals("хост под Sberl & PES", map.get("green[0].tslds-efs002603.ufsflcore.delta.sbrf.ru").getComment());
        assertEquals("VM", map.get("nginx_node_mm[0]").getComment());
    }

    @Test
    void flatToString_commentsMapping() throws Exception {
        var item1 = new FileDataItem();
        item1.setKey("key1");
        item1.setValue("value1");
        item1.setComment("comment1");

        var item2 = new FileDataItem();
        item2.setKey("key2");
        item2.setValue("value2");
        item2.setComment("comment2");

        var item3 = new FileDataItem();
        item3.setKey("key3");
        item3.setValue("value3");

        HashMap<String, FileDataItem> map = new HashMap<>();
        map.put("key1", item1);
        map.put("key2", item2);
        map.put("key3", item3);

        var actual = flatIni.flatToString(map);

        var expected = Files.readString(Paths.get(EXPECTED_4));

        assertEquals(expected, actual);
    }
}
