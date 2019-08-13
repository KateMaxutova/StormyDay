package my.katerina.task.storm;

import my.katerina.task.msg.ComputedLog;
import my.katerina.task.msg.ErrorLog;
import my.katerina.task.msg.LogLevel;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

public class ErrorValidationBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        for (ComputedLog compLog : (List<ComputedLog>) tuple.getValue(0)) {
            double avg = compLog.getAvgForWindow();
            if (compLog.getLevel() == LogLevel.ERROR && avg > 1) {
                basicOutputCollector.emit(new Values(new ErrorLog(compLog.getHost(), avg)));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
