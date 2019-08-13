package my.katerina.task.storm;

import my.katerina.task.kafka.KafkaConstants;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class LogTopology {
    private static final int TIMEOUT_SECS = 20;
    private static final int WINDOW_SIZE_SECS = 60;
    private static final int WINDOW_SLIDE_SECS = 0;

    public void submitTopologyLocalCluster() {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", getConfig(), getLogTopology());
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setMessageTimeoutSecs(TIMEOUT_SECS);
        return config;
    }

    /**
     * @return the topology to run
     */
    protected StormTopology getLogTopology() {
        final TopologyBuilder tp = new TopologyBuilder();

        // consume from the logs topic
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig()));

        BaseWindowedBolt.Duration windowDuration = BaseWindowedBolt.Duration.seconds(WINDOW_SIZE_SECS);
        BaseWindowedBolt.Duration slideDuration = BaseWindowedBolt.Duration.seconds(WINDOW_SLIDE_SECS);

        tp.setBolt("sliding_window", new SlidingWindowAvgBolt().withWindow(windowDuration, slideDuration))
                .shuffleGrouping("kafka_spout");

        tp.setBolt("error_log", new ErrorValidationBolt())
                .shuffleGrouping("sliding_window");

        tp.setBolt("kakfa_bolt", new KafkaProducerBolt(KafkaConstants.TOPIC_NAME_AVG))
                .shuffleGrouping("sliding_window");

        tp.setBolt("kakfa_err_bolt", new KafkaProducerBolt(KafkaConstants.TOPIC_NAME_ERROR))
                .shuffleGrouping("error_log");

        return tp.createTopology();
    }

    protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig() {
        return KafkaSpoutConfig.builder(KafkaConstants.KAFKA_BROKERS, KafkaConstants.TOPIC_NAME_LOGS).build();
    }


}
