package my.katerina.task.storm;

import my.katerina.task.kafka.ProducerBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.concurrent.ExecutionException;

public class KafkaProducerBolt extends BaseBasicBolt {
    private String kafkaTopic;

    public KafkaProducerBolt(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Producer<Long, String> producer = ProducerBuilder.build();
        ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(kafkaTopic,
                tuple.getString(0));
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Record sent: " + tuple.getString(0));
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
