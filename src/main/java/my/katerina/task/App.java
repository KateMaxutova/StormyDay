package my.katerina.task;

import my.katerina.task.kafka.ConsumerBuilder;
import my.katerina.task.kafka.KafkaConstants;
import my.katerina.task.kafka.ProducerBuilder;
import my.katerina.task.msg.LogMessageGenerator;
import my.katerina.task.storm.LogTopology;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class App {

    public static void main(String[] args) {
//        runProducer();
//        runConsumer();
        new LogTopology().submitTopologyLocalCluster();
    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerBuilder.build();
        consumer.subscribe(Collections.singletonList(KafkaConstants.TOPIC_NAME_LOGS));

        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            //print each record.
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }

    static void runProducer() {
        Producer<Long, String> producer = ProducerBuilder.build();

        for (int index = 0; index < KafkaConstants.MESSAGE_COUNT; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<>(
                    KafkaConstants.TOPIC_NAME_LOGS,
                    LogMessageGenerator.generateLog().toString()
            );

            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }
}
