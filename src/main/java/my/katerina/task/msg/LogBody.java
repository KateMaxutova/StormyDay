package my.katerina.task.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogBody {
    private long timestamp;
    private String host;
    private LogLevel level;
    private String text;
}

