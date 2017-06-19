package cl.usach.storm.npl.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;

import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import cl.usach.storm.npl.eda.Tweet;

public class QueryTweets {
	
	
	public static void mongoToKafka(){
		/**
		 * FALTA GUARDAR TODOS LOS EVENTOS EN KAFKA, ACTUALMENTE HAY ALREDEDOR DE 1000000
		 * EJECUTAR SIN: Document limit = new Document ("$limit", 1000000);   
		 */
		//obtener los tweets almacenados en mongo e insertarlos en kafka
		//kafka config
		Properties propsQty = new Properties();
		propsQty.put("bootstrap.servers", "localhost:9092");
		propsQty.put("acks", "all");
		propsQty.put("retries", 0);
		propsQty.put("batch.size", 16384);
		propsQty.put("linger.ms", 1);
		propsQty.put("buffer.memory", 33554432);
		propsQty.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		propsQty.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(propsQty);
		               
		List<Integer> sendQty = new ArrayList<Integer>();
		List<Tweet> tweets = new ArrayList<Tweet>();
		
    	//queue = new LinkedBlockingQueue<Transaction>(1000000);
		//this.collector = collector;
		Gson gson = new Gson();
		
    	try(MongoClient mongoClient = new MongoClient()){
    		MongoDatabase db = mongoClient.getDatabase("storm-world-cup");
    		MongoCollection<Document> collection = db.getCollection("tweets");
    		//Document limit = new Document ("$limit", 1000000);
    		
    		Document group = new Document("$group", 
		    		new Document("_id", 
		    				new Document("month", new Document("$month","$created_at"))
		    				.append("year", new Document("$year","$created_at"))
		    				.append("day", new Document("$dayOfMonth", "$created_at"))
		    				.append("hour", new Document("$hour","$created_at"))
		    				.append("minute", new Document("$minute","$created_at"))
		    				//.append("second", new Document("$second","$created_at"))
		    		).append("click", new Document("$sum",1))
		    );
		    Document sort = new Document("$sort",
		    		new Document("year", 1));
		    
		    Block<Document> printBlock = new Block<Document>() {
    	        @Override
    	        public void apply(final Document document) {
    	        	sendQty.add(document.getInteger("click"));
    	        }
    	    };
    	    
			collection.aggregate(Arrays.asList(group,sort)).allowDiskUse(true).forEach(printBlock);
			
			Document project = new Document("$project", 
					new Document("_id",0)
					.append("created_at", new Document("$dateToString", new Document("format", "%Y-%m-%d %H:%M:%S").append("date","$created_at") ))
					.append("text",1)
					.append("user",1));
			
			Document sortData = new Document("$sort",
					new Document("created_at", 1));

			AggregateIterable<Document> data = collection.aggregate(Arrays.asList(project,sortData)).allowDiskUse(true);
			System.out.println("<<PRODUCING TWEETS>>");
			for (Document document : data) {
				Tweet tweet = new Tweet(((Document) document.get("user")).get("screen_name").toString(), document.get("text").toString(), document.get("created_at").toString());
				//System.out.println(tweet);
				producer.send(new ProducerRecord<String, String>("tweets", "tweet", gson.toJson(tweet)));
			}
			System.out.println("<<PRODUCING TWEETSQTY>>");
			for (Integer integer : sendQty) {
				//System.out.println(integer);
				//producer.send(new ProducerRecord<String, String>("tweetsQty", "qty", integer.toString()));
			}
			System.out.println("cycles: " + sendQty.size());
    	}
	}
	
		
	public static void testQueryKafka(){
		//programa utilizado para el spout kafka con manejo de eventos enviados y offsets
		//actualmente esta fallando en reenviar un evento
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
    	        			//evaluar cuantos eventos faltan por procesar del ciclo actual
    	        			
    	    	    		for (TopicPartition tp : records.partitions()) {
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
	        	    	    		long lastQty = 0;
	        	    	    		for (ConsumerRecord<String, String> recordInner : innerRecords) {
	        	    	    			lastQty = Long.parseLong(recordInner.value());
	        	    	    		}
	        	    	    		tweetQtyConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(qtyOffset)));
	        	    	    		
	        	    	    		//evaluar si el proximo tweet es el que sigue despues del lastQty
	        	    	    		tweetRecords = tweetsConsumer.poll(100);
	        	    	    		for (ConsumerRecord<String, String> tweetRecord: tweetRecords) {
	        	    	    			long actualOffset = tweetRecord.offset()+1;
	        	    	    			tweetsConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(actualOffset)));
	        	    	    			if(actualOffset-lastQty > 0){
	        	    	    				sendQty = sendQty - (actualOffset - lastQty);
	        	    	    			}
	            	            	}
	        	    	    		for(int i=0; i < sendQty; i++){
	        	        				tweetRecords = tweetsConsumer.poll(100);
	        	        				for (ConsumerRecord<String, String> tweetRecord: tweetRecords) {
	                	        			Tweet tweet = gson.fromJson(tweetRecord.value(), Tweet.class);
	                	            		System.out.println(tweetRecord.offset() + ": " + tweet);
	                	            	}
	            	        		}
	        	    	    		
	    	    	    		}
	    	    	    		
    	    	    		}
    	        		}else{
    	        			//sin reset, enviar los tweets que corresponda por sendQty
    	        			for(int i=0; i < sendQty; i++){
    	        				tweetRecords = tweetsConsumer.poll(100);
    	        				for (ConsumerRecord<String, String> tweetRecord: tweetRecords) {
            	        			Tweet tweet = gson.fromJson(tweetRecord.value(), Tweet.class);
            	            		System.out.println(tweetRecord.offset() + ": " + tweet);
            	            	}
        	        		}
    	        		}
    	        		firstCycle = false;
    	        	}
    	    	}    			
    		}
    		
    	}
	}
	
	public static void generateStaticQueueKafka(double seconds){
		
		Properties propsQty = new Properties();
		propsQty.put("bootstrap.servers", "localhost:9092");
		propsQty.put("acks", "all");
		propsQty.put("retries", 0);
		propsQty.put("batch.size", 16384);
		propsQty.put("linger.ms", 1);
		propsQty.put("buffer.memory", 33554432);
		propsQty.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		propsQty.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		double time = seconds; //segundos
		int window = 3;// cantidad de ventanas
		int eventsPerWindow = (int) Math.ceil(time / window);
		int events = 200;
		
		List<Integer> eventsQty = Arrays.asList(events,events*2,events);
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(propsQty);

		for (Integer qty : eventsQty) {
			for (int i = 0; i < eventsPerWindow; i++) {
				producer.send(new ProducerRecord<String, String>("tweetsQty", "qty", qty.toString()));
			}
		}
		
	}
	
	public static void resetKafkaOffset(){
		resetTweetsQtyOffset();
		resetTweetsOffset();
	}
	
	public static void resetTweetsQtyOffset(){
		//reset offset de topics tweets y tweetsQty (ejecutar 2 veces por si acaso)
		Properties propsQty = new Properties();
    	propsQty.put("bootstrap.servers", "localhost:9092");
    	propsQty.put("group.id", "app1");
    	propsQty.put("enable.auto.commit", "false");
    	propsQty.put("auto.offset.reset", "earliest");
    	propsQty.put("max.poll.records", 1);
    	propsQty.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	propsQty.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	
    	try(KafkaConsumer<String, String> tweetQtyConsumer = new KafkaConsumer<String, String>(propsQty);){
    		tweetQtyConsumer.subscribe(Arrays.asList("tweetsQty"));
    		tweetQtyConsumer.poll(1000);
	    	tweetQtyConsumer.seekToBeginning(Arrays.asList(new TopicPartition("tweetsQty", 0)));
	    	ConsumerRecords<String, String> records = tweetQtyConsumer.poll(1000);
    		for (TopicPartition tp : records.partitions()) {
    			tweetQtyConsumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(0)));
    			System.out.println(tweetQtyConsumer.position(new TopicPartition("tweetsQty", 0)));
			}
    	}
	}
	public static void resetTweetsOffset(){

    	Properties propsTwt = new Properties();
    	propsTwt.put("bootstrap.servers", "localhost:9092");
    	propsTwt.put("group.id", "app2");
    	propsTwt.put("enable.auto.commit", "false");
    	propsTwt.put("auto.offset.reset", "earliest");
    	propsTwt.put("max.poll.records", 1);
    	propsTwt.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	propsTwt.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		try(KafkaConsumer<String, String> tweetsConsumer = new KafkaConsumer<String, String>(propsTwt);){
	    	tweetsConsumer.subscribe(Arrays.asList("tweets"));
    		tweetsConsumer.poll(1000);
    		tweetsConsumer.seekToBeginning(Arrays.asList(new TopicPartition("tweets", 0)));
	    	ConsumerRecords<String, String> recordsTw = tweetsConsumer.poll(1000);
    		for (TopicPartition tp2 : recordsTw.partitions()) {
    			tweetsConsumer.commitSync(Collections.singletonMap(tp2, new OffsetAndMetadata(0)));
    			System.out.println(tweetsConsumer.position(new TopicPartition("tweets", 0)));
			}	
		}
	}
	
	public static void testOffsets(){
		Properties propsQty = new Properties();
    	propsQty.put("bootstrap.servers", "localhost:9092");
    	propsQty.put("group.id", "app1");
    	propsQty.put("enable.auto.commit", "false");
    	propsQty.put("auto.offset.reset", "earliest");
    	propsQty.put("max.poll.records", 1);
    	propsQty.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	propsQty.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	
    	Properties propsTwt = new Properties();
    	propsTwt.put("bootstrap.servers", "localhost:9092");
    	propsTwt.put("group.id", "app2");
    	propsTwt.put("enable.auto.commit", "false");
    	propsTwt.put("auto.offset.reset", "earliest");
    	propsTwt.put("max.poll.records", 1);
    	propsTwt.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	propsTwt.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    	
    	Gson gson = new Gson();
    	try(KafkaConsumer<String, String> tweetQtyConsumer = new KafkaConsumer<String, String>(propsQty);){
    		try(KafkaConsumer<String, String> tweetsConsumer = new KafkaConsumer<String, String>(propsTwt);){
    			
    			tweetQtyConsumer.subscribe(Arrays.asList("tweetsQty"));
    	    	tweetsConsumer.subscribe(Arrays.asList("tweets"));
    	    	
    	    	ConsumerRecords<String, String> records;
    	    	while((records = tweetQtyConsumer.poll(1000))!=null){
    	        	for (ConsumerRecord<String, String> record : records) {
    	        		System.out.println(record.offset() + ": " + record.value());
    	        		long qtyOffset = record.offset();
    	        		long sendQty = Long.parseLong(record.value());
    	        		
    	        		ConsumerRecords<String, String> tweetRecords = null;
    	        		for(int i=0; i < sendQty; i++){
	        				tweetRecords = tweetsConsumer.poll(1000);
	        				for (ConsumerRecord<String, String> tweetRecord: tweetRecords) {
        	        			Tweet tweet = gson.fromJson(tweetRecord.value(), Tweet.class);
        	            		System.out.println(tweetRecord.offset() + ": " + tweet);
        	            	}
    	        		}
    	        		
    	        	}
    	    	}    			
    		}
    		
    	}
	}

	public static void main(String[] args) {
		//generateStaticQueueKafka();
		
		resetKafkaOffset();

//		mongoToKafka();
//testOffsets();
		
		//mongoToKafka();
		//generateStaticQueueKafka(300);
		//testQueryKafka();
	}

}

