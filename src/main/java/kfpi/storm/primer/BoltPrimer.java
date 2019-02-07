package kfpi.storm.primer;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Map;
import java.util.Objects;


public class BoltPrimer extends BaseBasicBolt {

    private static final Logger log = LoggerFactory.getLogger(BoltPrimer.class);
    private String fileName;

    @Override
    public void prepare(Map stormConf,
                        TopologyContext context) {
        super.prepare(stormConf, context);
        fileName = String.valueOf(Objects.requireNonNull(
                stormConf.get("demo.file.out")));
    }

    @Override
    public void execute(Tuple input,
                        BasicOutputCollector collector) {
        try(final PrintWriter printWriter = new PrintWriter(
                new FileWriter(fileName, true))) {
            final String newLine = "processing line "
                    + new Date().getTime()
                    + ": "
                    + input.getStringByField("line");
            printWriter.println(newLine);
            log.info("line [{}] written to [{}]", newLine, fileName);
        } catch (Exception e) {
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to declare
    }
}
