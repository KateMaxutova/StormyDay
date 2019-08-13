package my.katerina.task.kafka;

public interface KafkaConstants {
    String KAFKA_BROKERS = "localhost:9092";
    Integer MESSAGE_COUNT = 1000;
    String CLIENT_ID = "client1";
    String TOPIC_NAME_LOGS = "logs";
    String TOPIC_NAME_AVG = "stat_avg";
    String TOPIC_NAME_ERROR = "errors";
    String GROUP_ID_CONFIG = "GroupA";
    Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    String OFFSET_RESET_EARLIER = "earliest";
    Integer MAX_POLL_RECORDS = 1;
}
