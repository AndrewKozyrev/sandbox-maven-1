import java.util.Map;

public interface FlatService {
    Map<String, FileDataItem> flatToMap(String data);

    String flatToString(Map<String, FileDataItem> data);

    void validate(Map<String, FileDataItem> data);
}
