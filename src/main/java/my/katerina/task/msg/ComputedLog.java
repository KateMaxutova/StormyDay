package my.katerina.task.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ComputedLog {
    private String host;
    private LogLevel level;
    private Integer sumForWindow;
    private Double avgForWindow;
}
