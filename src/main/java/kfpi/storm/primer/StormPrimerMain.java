package kfpi.storm.primer;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.TestUtils;

public class StormPrimerMain {

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout1", new SpoutPrimer());
        topologyBuilder.setBolt("bolt1", new BoltPrimer())
                .shuffleGrouping("spout1");

        StormTopology topology = topologyBuilder.createTopology();

        LocalCluster localCluster = new LocalCluster();
        Config conf = new Config();
        conf.put("demo.file.in", "C:\\Dev\\storm-primer\\in.txt");
        conf.put("demo.file.out", "C:\\Dev\\storm-primer\\out.txt");

        localCluster.submitTopology("PRIMER", conf, topology);

        TestUtils.sleep(60_000);

        localCluster.shutdown();
    }
}
