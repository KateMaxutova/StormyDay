package my.katerina.task.msg;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LogScheme implements Scheme {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        try {
            List<LogMessage> messages = mapper.readValue(
                    byteBuffer.array(),
                    mapper.getTypeFactory().constructCollectionType(List.class, LogMessage.class)
            );
            return messages.stream()
                    .map(LogMessage::toValues)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }
}
