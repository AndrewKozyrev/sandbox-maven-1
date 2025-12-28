package mappers;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class FlatIniTest {

    private static final String INPUT_1 = "src/test/resources/flat_mapper/ini/input_1";
    private static final String INPUT_2 = "src/test/resources/flat_mapper/ini/input_2";
    private static final String INPUT_3 = "src/test/resources/flat_mapper/ini/input_3";
    private static final String EXPECTED_1 = "src/test/resources/flat_mapper/ini/expected_1";
    private static final String EXPECTED_2 = "src/test/resources/flat_mapper/ini/expected_2";
    private static final String EXPECTED_3 = "src/test/resources/flat_mapper/ini/expected_3";
    private static final String EXPECTED_4 = "src/test/resources/flat_mapper/ini/expected_4";
    private static final String EXPECTED_5 = "src/test/resources/flat_mapper/ini/expected_5";
    private static final String EXPECTED_6 = "src/test/resources/flat_mapper/ini/expected_6";
    private static final String EXPECTED_7 = "src/test/resources/flat_mapper/ini/expected_7";
    private static final String EXPECTED_8 = "src/test/resources/flat_mapper/ini/expected_8";
    private static final String EXPECTED_9 = "src/test/resources/flat_mapper/ini/expected_9";
    private static final String EXPECTED_10 = "src/test/resources/flat_mapper/ini/expected_10";
    private static final String EXPECTED_11 = "src/test/resources/flat_mapper/ini/expected_11";

    @Test
    void mapsCorrectly() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);

        assertEquals("ansible_connection=local", map.get("local[0].localhost").getValue());
        assertNull(map.get("local[0].localhost").getComment());

        assertEquals("ansible_host=tslds-efs002568.delta.sbrf.ru ansible_user=\"{{ SSD_SSH_USER }}\" ansible_ssh_private_key_file=\"{{ vm_ssd_ssh_cred }}\"", map.get("blue[0].tslds-efs002568.ufsflcore.delta.sbrf.ru").getValue());
        var comment1 = "#хост под Sberl\n" +
                "#tklds-efs001350.ufsflcore.delta.sbrf.ru ansible_host=tklds-efs001350.ufsflcore.delta.sbrf.ru ansible_user=\"{{ SSD_SSH_USER }}\" ansible_ssh_private_key_file=\"{{ vm_ssd_ssh_cred }}\"\n" +
                "#PES";
        assertEquals(comment1, map.get("blue[0].tslds-efs002568.ufsflcore.delta.sbrf.ru").getComment());

        assertEquals("tslds-efs002568.ufsflcore.delta.sbrf.ru", map.get("nginx_node_mm[0]").getValue());
        var comment2 = "#VM\n" +
                "#tklds-efs001350.ufsflcore.delta.sbrf.ru\n" +
                "#tklds-efs001351.ufsflcore.delta.sbrf.ru\n" +
                "#PES";
        assertEquals(comment2, map.get("nginx_node_mm[0]").getComment());

        assertEquals("tslds-efs002569.ufsflcore.delta.sbrf.ru", map.get("nginx_node_mm[1]").getValue());
        assertNull(map.get("nginx_node_mm[1]").getComment());

        assertNull(map.get("\u0000__flat_ini_meta__").getComment());
        assertNotNull(map.get("\u0000__flat_ini_meta__").getValue());
    }

    @Test
    void mapsCorrectly2() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_3));
        var map = new FlatIni().flatToMap(inputData);

        assertEquals("value_1", map.get("section_1[0].param_1").getValue());
        assertEquals("# comment part 1\n#comment part 2\n## comment part 3", map.get("section_1[0].param_1").getComment());

        assertEquals("value_2", map.get("section_1[1].param_2").getValue());
        assertNull(map.get("section_1[1].param_2").getComment());

        assertEquals("value_3", map.get("section_1[2].param_3").getValue());
        assertEquals("# comment for param_3", map.get("section_1[2].param_3").getComment());

        assertEquals("param_4", map.get("section_2[0]").getValue());
        assertNull(map.get("section_2[0]").getComment());

        assertEquals("param_5", map.get("section_2[1]").getValue());
        assertNull(map.get("section_2[1]").getComment());

        assertFalse(map.containsKey("section[3]"));
    }

    @Test
    void unflattensCorrectly() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);

        var actual = new FlatIni().flatToString(map);
        assertEquals(inputData, actual);
    }

    @Test
    void singleParameter() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_2));
        var map = new FlatIni().flatToMap(inputData);

        assertNotNull(map.get("param1"));
        assertEquals("value1", map.get("param1").getValue());
        assertNull(map.get("param1").getComment());
        assertNull(map.get("\u0000__flat_ini_meta__").getComment());
        assertNotNull(map.get("\u0000__flat_ini_meta__").getValue());
    }

    @Test
    void correctOrderOfParams() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));

        var map = new FlatIni().flatToMap(inputData);

        var actual = new ArrayList<>(map.values());

        for (FileDataItem fileDataItem : actual) {
            System.out.println(fileDataItem.getKey() + " = " + fileDataItem.getValue());
        }
        var expected = Files.readAllLines(Paths.get(EXPECTED_1))
                .stream()
                .map(x -> x.split("\\s*=\\s*", 2))
                .map(x -> new SimpleEntry<>(x[0], x[1]))
                .collect(Collectors.toList());
        for (int i = 0; i < actual.size(); i++) {
            var actualKey = actual.get(i).getKey();
            if (actualKey.equals("\u0000__flat_ini_meta__")) {
                continue;
            }
            var expectedKey = expected.get(i).getKey();
            assertEquals(expectedKey, actualKey);
            var actualValue = actual.get(i).getValue();
            var expectedValue = expected.get(i).getValue();
            assertEquals(expectedValue, actualValue);
        }
    }

    @Test
    void commentsMapping() throws Exception {
        var item1 = new FileDataItem();
        item1.setKey("key1");
        item1.setValue("value1");
        item1.setComment("# comment1");

        var item2 = new FileDataItem();
        item2.setKey("key2");
        item2.setValue("value2");
        item2.setComment("# comment2");

        var item3 = new FileDataItem();
        item3.setKey("key3");
        item3.setValue("value3");

        HashMap<String, FileDataItem> map = new HashMap<>();
        map.put("key1", item1);
        map.put("key2", item2);
        map.put("key3", item3);

        var actual = new FlatIni().flatToString(map);

        var expected = Files.readString(Paths.get(EXPECTED_2));

        assertEquals(expected, actual);
    }

    @Test
    void removingParameter() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);
        map.remove("green[0].tslds-efs002569.ufsflcore.delta.sbrf.ru");
        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_3));
        assertEquals(expected, actual);
    }

    @Test
    void removingParameter2() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);

        map.remove("nginx_node_mm[0]");

        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_10));
        assertEquals(expected, actual);
    }

    @Test
    void simpleSection() throws Exception {
        var item1 = new FileDataItem();
        item1.setKey("section1[0]");
        item1.setValue("param1=value1");
        item1.setComment("# comment1");

        var item2 = new FileDataItem();
        item2.setKey("section1[1]");
        item2.setValue("param2=value2");
        item2.setComment("# comment2");

        var item3 = new FileDataItem();
        item3.setKey("section1[2]");
        item3.setValue("param3=value3");

        HashMap<String, FileDataItem> map = new HashMap<>();
        map.put("section1[0]", item1);
        map.put("section1[1]", item2);
        map.put("section1[2]", item3);

        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_4));
        assertEquals(expected, actual);
    }

    @Test
    void afterModifyingValue() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);

        map.get("vm_sds_master:children[0]").setValue("red");

        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_5));
        assertEquals(expected, actual);
    }

    @Test
    void afterAddingComment() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);

        map.get("vm_sds_master:children[1]").setValue("red");
        map.get("vm_sds_master:children[1]").setComment("# NEW COMMENT WITH EDIT PARAM");

        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_6));
        assertEquals(expected, actual);
    }

    @Test
    void addingParameterWithCommentToExistingEmptyBlock() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);

        map.put(
                "nginx_mm[0]",
                FileDataItem.builder()
                        .key("nginx_mm[0]")
                        .value("nginx_mm_value_1")
                        .comment("# NEW BLOCK COMMENT")
                        .build()
        );

        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_7));
        assertEquals(expected, actual);
    }

    @Test
    void addingNewItem() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);

        map.put(
                "nginx_node_mm[3]",
                FileDataItem.builder()
                        .key("nginx_node_mm[3]")
                        .value("value_for_nginx_node_mm[3]_item")
                        .build()
        );

        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_8));
        assertEquals(expected, actual);
    }

    @Test
    void editingComment() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_1));
        var map = new FlatIni().flatToMap(inputData);

        map.get("green[0].tslds-efs002569.ufsflcore.delta.sbrf.ru").setComment("# modified comment");

        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_9));
        assertEquals(expected, actual);
    }

    @Test
    void keepingEmptyLines() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_3));
        var map = new FlatIni().flatToMap(inputData);

        map.remove("section_2[0]");
        map.remove("section_2[1]");

        var actual = new FlatIni().flatToString(map);
        var expected = Files.readString(Paths.get(EXPECTED_11));
        assertEquals(expected, actual);
    }
}
