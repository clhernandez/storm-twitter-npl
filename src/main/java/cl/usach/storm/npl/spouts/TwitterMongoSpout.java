package cl.usach.storm.npl.spouts;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import cl.usach.storm.npl.eda.MongoTweet;
import cl.usach.storm.npl.eda.Tweet;

/**
 * Created by clhernandez on 01-06-17.
 */
public class TwitterMongoSpout extends BaseRichSpout{
	private static TwitterMongoSpout instance;
	private TwitterMongoSpout(){}
	public static TwitterMongoSpout getInstance(){
		if(instance==null){
			instance = new TwitterMongoSpout();
		}
		return instance;
	}
	
	private MongoTweet mongoTweet;
	private SpoutOutputCollector collector;
	
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
    	this.collector = collector;
    	
    	System.out.println("INIT");
    	try {
			mongoTweet = MongoTweet.getInstance();
			System.out.println("INIT QUEUE: " + mongoTweet.getQueueSize());
	    	Integer send;
	    	while((send = mongoTweet.getSendQty()) != null){
	    		for (int i = 0; i < send; i++) {
					nextTuple();
				}
	    		System.out.println("<<SLEEP>>");
				Utils.sleep(500);
	    	}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void activate() {
        super.activate();
    }

    @Override
    public void deactivate() {
        super.deactivate();
    }

    @Override
    public void nextTuple() {
    	Tweet q = mongoTweet.getTweet();
    	System.out.println(q.getUser());
		if (q == null) {
			Utils.sleep(1);
        } else {
			collector.emit(new Values(q));
		}
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("tweet"));
    }
}
