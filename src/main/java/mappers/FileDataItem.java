package mappers;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class FileDataItem {
    private String key;
    private Object value;
    private String path;
    private String filename;
    private String comment;
}
