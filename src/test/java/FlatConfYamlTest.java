import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlatConfYamlTest {

    private static final String INPUT_CONF_YAML = "src/test/resources/flat_mapper/yaml/custom_property.conf.yml";
    private static final String FLAT_CONF_YAML = "src/test/resources/flat_mapper/yaml/flattened_custom_property_flat_conf_yml.txt";
    private static final String RECONSTRUCTED_CONF_YAML = "src/test/resources/flat_mapper/yaml/reconstructed_custom_property.conf.yml";
    private static final String INPUT_7 = "src/test/resources/flat_mapper/yaml/input_7.conf.yml";

    @Test
    void flatToMap_flattensCorrectly() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_CONF_YAML));
        var items = new FlatConfYaml().flatToMap(inputData);
        var expectedData = Files.readAllLines(Paths.get(FLAT_CONF_YAML))
                .stream()
                .map(x -> x.split(":\\s", 2))
                .collect(Collectors.toMap(x -> x[0], y -> y[1]));
        for (String key : items.keySet()) {
            if (key.equals("\u0000__flat_conf_yml_meta__")) continue;
            var expectedValue = expectedData.get(key);
            var actualValue = items.get(key).getValue().toString();
            assertEquals(expectedValue, actualValue, String.format("Key = %s", key));
        }
    }

    @Test
    void flatToString_unflattensCorrectly() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_CONF_YAML));
        var items = new FlatConfYaml().flatToMap(inputData);
        var actual = new FlatConfYaml().flatToString(items);
        var expected = Files.readString(Paths.get(RECONSTRUCTED_CONF_YAML));
        assertEquals(expected, actual);
    }

    @Test
    void preserveEmptyLinesAfterEdit() throws Exception {
        var inputData = Files.readString(Paths.get(INPUT_7));
        var map = new FlatConfYaml().flatToMap(inputData);

        var actual = new FlatConfYaml().flatToString(map);

        assertEquals(inputData, actual);
    }

}
