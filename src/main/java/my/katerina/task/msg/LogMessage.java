package my.katerina.task.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.storm.tuple.Values;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogMessage {
    private LogBody value;

    public Values toValues() {
        return new Values(
                value.getTimestamp(),
                value.getHost(),
                value.getLevel(),
                value.getText()
        );
    }
}
