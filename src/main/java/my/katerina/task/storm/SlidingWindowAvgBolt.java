package my.katerina.task.storm;

import my.katerina.task.msg.ComputedLog;
import my.katerina.task.msg.LogBody;
import my.katerina.task.msg.LogLevel;
import my.katerina.task.msg.LogMessage;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SlidingWindowAvgBolt extends BaseWindowedBolt {
    private static final double WINDOW_SIZE_SECS = 60.0;

    private Map<Pair<String, LogLevel>, Integer> eventStat = new HashMap<>();
    private OutputCollector collector;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();

        System.out.println("Events in current window: " + tuplesInWindow.size());

        for (Tuple tuple : newTuples) {
            LogBody logMsg = ((LogMessage) tuple.getValue(0)).getValue();
            eventStat.forEach((event, value) -> {
                if (event.getFirst().equals(logMsg.getHost()) && event.getSecond() == logMsg.getLevel()) {
                    eventStat.put(event, value + 1);
                }
            });
        }
        for (Tuple tuple : expiredTuples) {
            LogBody logMsg = ((LogMessage) tuple.getValue(0)).getValue();
            eventStat.forEach((event, value) -> {
                if (event.getFirst().equals(logMsg.getHost()) && event.getSecond() == logMsg.getLevel()) {
                    eventStat.put(event, value - 1);
                }
            });
        }
        List<ComputedLog> computedLogs = new LinkedList<>();
        eventStat.forEach((event, value) -> {
            computedLogs.add(new ComputedLog(event.getFirst(), event.getSecond(), value, value / WINDOW_SIZE_SECS));
        });

        collector.emit(new Values(computedLogs));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("computedLogs"));
    }
}
