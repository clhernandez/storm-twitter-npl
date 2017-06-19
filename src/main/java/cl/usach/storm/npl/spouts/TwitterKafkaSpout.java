package cl.usach.storm.npl.spouts;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.google.gson.Gson;

import cl.usach.storm.npl.eda.Tweet;

/**
 * Created by clhernandez on 01-06-17.
 */
public class TwitterKafkaSpout extends BaseRichSpout{
	
	private LinkedBlockingQueue<Tweet> queue;
	private SpoutOutputCollector collector;
	
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
    	this.collector = collector;
    	queue = new LinkedBlockingQueue<Tweet>(100000);

    	Properties propsQty = new Properties();
    	propsQty.put("bootstrap.servers", "localhost:9092");
    	propsQty.put("group.id", "app1");
    	propsQty.put("enable.auto.commit", "true");
    	propsQty.put("auto.offset.reset", "earliest");
    	propsQty.put("max.poll.records", 1);
    	propsQty.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	propsQty.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	
    	Properties propsTwt = new Properties();
    	propsTwt.put("bootstrap.servers", "localhost:9092");
    	propsTwt.put("group.id", "app2");
    	propsTwt.put("enable.auto.commit", "true");
    	propsTwt.put("auto.offset.reset", "earliest");
    	propsTwt.put("max.poll.records", 1);
    	propsTwt.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	propsTwt.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    	Gson gson = new Gson();
    	try(KafkaConsumer<String, String> tweetQtyConsumer = new KafkaConsumer<String, String>(propsQty);){
    		try(KafkaConsumer<String, String> tweetsConsumer = new KafkaConsumer<String, String>(propsTwt);){
    			
    			tweetQtyConsumer.subscribe(Arrays.asList("tweetsQty"));
    	    	tweetsConsumer.subscribe(Arrays.asList("tweets"));
    	    	
    	    	//Map<String, List<PartitionInfo>> asd = tweetQtyConsumer.listTopics();
    	    	//Map<String, List<PartitionInfo>> asd2 = tweetsConsumer.listTopics();
    	    	
    	    	ConsumerRecords<String, String> records;
    	    	boolean firstCycle = true;
    	    	while((records = tweetQtyConsumer.poll(100))!=null){
    	        	for (ConsumerRecord<String, String> record : records) {
    	        		System.out.println(record.offset() + ": " + record.value());
    	        		long qtyOffset = record.offset();
    	        		long sendQty = Long.parseLong(record.value());
    	        		
    	        		ConsumerRecords<String, String> tweetRecords = null;
    	        		if(qtyOffset>0 && firstCycle){
    	        			System.out.println("<<RESET KAFKA SPOUT>>");
    	        			//evaluar cuantos eventos faltan por procesar del ciclo actual
    	        			
    	    	    		for (TopicPartition tp : records.partitions()) {
    	    	    			//para obtener la cantidad de eventos a enviar anterior
    	    	    			tweetQtyConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(qtyOffset-1)));
	    	    				
	    	    	    		Properties innerPropsQty = new Properties();
	    	    	        	innerPropsQty.put("bootstrap.servers", "localhost:9092");
	    	    	        	innerPropsQty.put("group.id", "appInner");
	    	    	        	innerPropsQty.put("enable.auto.commit", "false");
	    	    	        	innerPropsQty.put("auto.offset.reset", "earliest");
	    	    	        	innerPropsQty.put("max.poll.records", 1);
	    	    	        	innerPropsQty.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    	    	        	innerPropsQty.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    	    	        	
	    	    	    		try(KafkaConsumer<String, String> tweetQtyInnerConsumer = new KafkaConsumer<String, String>(innerPropsQty);){
	    	    	    			tweetQtyInnerConsumer.subscribe(Arrays.asList("tweetsQty"));
	    	    	    			
	    	    	    			ConsumerRecords<String, String> innerRecords = tweetQtyInnerConsumer.poll(100);
	        	    	    		long lastQty = 0;//obtener la cantidad de eventos a enviar anterior
	        	    	    		for (ConsumerRecord<String, String> recordInner : innerRecords) {
	        	    	    			lastQty = Long.parseLong(recordInner.value());
	        	    	    		}
	        	    	    		tweetQtyConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(qtyOffset)));
	        	    	    		
	        	    	    		//evaluar si el proximo tweet es el que sigue despues del lastQty
	        	    	    		tweetRecords = tweetsConsumer.poll(100);
	        	    	    		for (ConsumerRecord<String, String> tweetRecord: tweetRecords) {
	        	    	    			long actualOffset = tweetRecord.offset()+1;
	        	    	    			//volver a dejar el offset donde corresponde
	        	    	    			tweetsConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(actualOffset)));
	        	    	    			if(actualOffset-lastQty > 0){
	        	    	    				sendQty = sendQty - (actualOffset - lastQty);//calcular cuantos eventos falta enviar este ciclo
	        	    	    			}
	            	            	}
	        	    	    		for(int i=0; i < sendQty; i++){
	        	    	    			//re enviar los eventos restantes del ciclo
	        	        				tweetRecords = tweetsConsumer.poll(100);
	        	        				for (ConsumerRecord<String, String> tweetRecord: tweetRecords) {
	        	        					Tweet tweet = gson.fromJson(tweetRecord.value(), Tweet.class);
	        	        					System.out.println(">>EMMIT AGAIN" + tweet);
	                	        			try {
	        									queue.put(tweet);
	        								} catch (InterruptedException e) {
	        									e.printStackTrace();
	        								}
	                	        			nextTuple();
	                	            	}
	            	        		}
	        	    	    		System.out.println("<<SLEEP>>");
	            					Utils.sleep(1000);
	        	    	    		
	    	    	    		}
	    	    	    		
    	    	    		}
    	        		}else{
    	        			//sin reset, enviar los tweets que corresponda por sendQty
    	        			for(int i=0; i < sendQty; i++){
    	        				tweetRecords = tweetsConsumer.poll(100);
    	        				for (ConsumerRecord<String, String> tweetRecord: tweetRecords) {
    	        					Tweet tweet = gson.fromJson(tweetRecord.value(), Tweet.class);
            	        			try {
    									queue.put(tweet);
    								} catch (InterruptedException e) {
    									e.printStackTrace();
    								}
            	        			nextTuple();
            	            	}
        	        		}
    	        			System.out.println("<<SLEEP>>");
        					Utils.sleep(1000);
    	        		}
    	        		firstCycle = false;
    	        	}
    	    	}    			
    		}
    		
    	}
    }

    @Override
    public void close() {
    	System.out.println("<<CLOSE>>");
        super.close();
    }

    @Override
    public void activate() {
    	System.out.println("<<ACTIVATE>>");
        super.activate();
    }

    @Override
    public void deactivate() {
    	System.out.println("<<DEACTIVATE>>");
        super.deactivate();
    }

    @Override
    public void nextTuple() {
    	Tweet q = queue.poll();
    	System.out.println(q);
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
