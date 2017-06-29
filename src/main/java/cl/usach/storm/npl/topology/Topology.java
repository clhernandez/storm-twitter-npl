package cl.usach.storm.npl.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import cl.usach.storm.npl.bolts.LanguageDetectorBolt;
import cl.usach.storm.npl.bolts.MongoBolt;
import cl.usach.storm.npl.bolts.SentimentAnalysisBolt;
import cl.usach.storm.npl.spouts.TwitterKafkaSpout;

/**
 * Topology class that sets up the Storm topology for this sample.
 */
public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-npl";

	public static void main(String[] args) {
		Config config = new Config();
		//config.setNumWorkers(1);//# of JVM
		config.setMessageTimeoutSecs(120);
		config.setNumAckers(0);
		config.setNumEventLoggers(0);

		TopologyBuilder b = new TopologyBuilder();
		
		b.setSpout("TwitterSpout", new TwitterKafkaSpout(),1);
		//b.setSpout("TwitterSpout", TwitterMongoSpout.getInstance(),1);
		//b.setSpout("TwitterSpout", new TwitterSampleSpout(),1);
		//b.setSpout("TwitterSampleSpout", new TwitterSampleSpout(),1);
		b.setBolt("LanguageDetectorBolt", new LanguageDetectorBolt(),1).setNumTasks(5).fieldsGrouping("TwitterSpout",new Fields("tweet"));
		b.setBolt("SentimentAnalysisBolt", new SentimentAnalysisBolt(),3).setNumTasks(10).fieldsGrouping("LanguageDetectorBolt", new Fields("user"));
		b.setBolt("MongoBolt", new MongoBolt(),3).setNumTasks(10).fieldsGrouping("SentimentAnalysisBolt", new Fields("user"));
	
		if (args != null && args.length > 0) {
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, config, b.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			final LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());
			Utils.sleep(2000000);
			cluster.shutdown();
			
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					cluster.killTopology(TOPOLOGY_NAME);
					cluster.shutdown();
				}
			});
		}
	}
}
