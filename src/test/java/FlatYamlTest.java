import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlatYamlTest {

    private static final String ORIGINAL_1 = "src/test/resources/flat_mapper/yaml/original_1.yaml";
    private static final String ORIGINAL_2 = "src/test/resources/flat_mapper/yaml/original_2.yaml";
    private static final String EXPECTED_FLAT_FILE_1 = "src/test/resources/flat_mapper/yaml/flattened_1.txt";
    private static final String EXPECTED_1 = "src/test/resources/flat_mapper/yaml/expected_1.yaml";
    private static final String EXPECTED_2 = "src/test/resources/flat_mapper/yaml/expected_2.yaml";
    private static final String INPUT_2 = "src/test/resources/flat_mapper/yaml/input_2.yaml";
    private static final String EXPECTED_3 = "src/test/resources/flat_mapper/yaml/expected_3.yaml";

    private final FlatYaml flatYaml = new FlatYaml();

    @Test
    void flatToMap_flattensCorrectly() throws Exception {
        var inputData = Files.readString(Paths.get(ORIGINAL_1));
        var items = flatYaml.flatToMap(inputData);
        var expectedData = Files.readAllLines(Paths.get(EXPECTED_FLAT_FILE_1))
                .stream()
                .map(x -> x.split(":\\s"))
                .collect(Collectors.toMap(x -> x[0], y -> y[1]));
        for (String key : items.keySet()) {
            var expectedValue = expectedData.get(key);
            var actualValue = items.get(key).getValue().toString();
            assertEquals(expectedValue, actualValue);
        }
    }

    @Test
    void flatToString_restoresOriginal() throws Exception {
        var inputData = Files.readString(Paths.get(ORIGINAL_1));
        var items = flatYaml.flatToMap(inputData);

        var actual = flatYaml.flatToString(items);
        var expected = Files.readString(Paths.get(EXPECTED_1));
        assertEquals(expected, actual);
    }

    @Test
    void flatToString_modifiedMap_unflattensCorrectly() throws Exception {
        var inputData = Files.readString(Paths.get(ORIGINAL_2));
        var items = flatYaml.flatToMap(inputData);
        items.get("apiVersion").setComment("comment1");
        items.get("kind").setComment("comment2");
        items.put("version", FileDataItem.builder().key("version").value("3.0.0").build());
        items.put("server.name", FileDataItem.builder().key("server.name").value("Apache Tomcat").comment("Information about server").build());
        var actual = flatYaml.flatToString(items);
        var expected = Files.readString(Paths.get(EXPECTED_2));
        assertEquals(expected, actual);
    }

    @Test
    void flatToString_inBetweenFlatMapThenRestore() throws Exception {
        var inputData = Files.readString(Paths.get(ORIGINAL_2));
        var items = flatYaml.flatToMap(inputData);

        var inputData2 = Files.readString(Paths.get(ORIGINAL_1));
        flatYaml.flatToMap(inputData2);

        items.get("apiVersion").setComment("comment1");
        items.get("kind").setComment("comment2");
        items.put("version", FileDataItem.builder().key("version").value("3.0.0").build());
        items.put("server.name", FileDataItem.builder().key("server.name").value("Apache Tomcat").comment("Information about server").build());

        var actual = flatYaml.flatToString(items);
        var expected = Files.readString(Paths.get(EXPECTED_2));
        assertEquals(expected, actual);
    }

    @Test
    void flatToString_exclamationMark() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_2));
        var items = flatYaml.flatToMap(inputData);

        var oldValue = items.get("param1").getValue();
        items.get("param1").setValue("!" + oldValue);

        var actual = flatYaml.flatToString(items);
        var expected = Files.readString(Paths.get(EXPECTED_3));

        assertEquals(expected, actual);
    }

}
