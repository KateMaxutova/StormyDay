package my.katerina.task.msg;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class LogMessageGenerator {
    private static final List<String> HOSTS = Arrays.asList("ya.ru", "google.ru", "my.org", "youtu.be", "apache.org");
    private static final Random RANDOM = new Random();

    public static LogMessage generateLog() {
        LogBody body = new LogBody();
        body.setTimestamp(System.currentTimeMillis());
        body.setHost(HOSTS.get(RANDOM.nextInt(HOSTS.size())));
        body.setLevel(LogLevel.values()[RANDOM.nextInt(HOSTS.size())]);
        body.setText(body.toString());

        return new LogMessage(body);
    }
}
