package cl.usach.storm.npl.eda;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoTweet {
	private static MongoTweet instance = null;	
	private static LinkedBlockingQueue<Tweet> queue;
	private static LinkedBlockingQueue<Integer> sendQty;
	
	private MongoTweet() throws Exception{
		try(MongoClient mongoClient = new MongoClient()){
    		MongoDatabase db = mongoClient.getDatabase("storm-world-cup");
    		MongoCollection<Document> collection = db.getCollection("tweets");
    	    
    		queue = new LinkedBlockingQueue<Tweet>(10000000);
    	    
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
		    
		    List<Integer> sq = new ArrayList<Integer>();
		    Block<Document> printBlock = new Block<Document>() {
    	        @Override
    	        public void apply(final Document document) {
    	        	sq.add(document.getInteger("click"));
    	        }
    	    };
    	    
			collection.aggregate(Arrays.asList(group,sort)).allowDiskUse(true).forEach(printBlock);
			sendQty = new LinkedBlockingQueue<Integer>(sq.size());
			for (Integer qty : sq) {
				sendQty.add(qty);
			}

			Document project = new Document("$project", 
					new Document("_id",0)
					.append("created_at", new Document("$dateToString", new Document("format", "%Y-%m-%d %H:%M:%S").append("date","$created_at") ))
					.append("text",1)
					.append("user",1));
			
			Document sortData = new Document("$sort",
					new Document("created_at", 1));
			System.out.println("<<LOADING TWEETS FROM MONGODB>>");
			AggregateIterable<Document> data = collection.aggregate(Arrays.asList(project,sortData)).allowDiskUse(true);

			for (Document document : data) {
				Tweet tweet = new Tweet(((Document) document.get("user")).get("screen_name").toString(), document.get("text").toString(), document.get("created_at").toString());
				queue.add(tweet);
			}
    	}
	}
	
	public static MongoTweet getInstance() throws Exception{
		if (instance == null) {
			instance = new MongoTweet();
		}
		return instance;
	}
	
	public Tweet getTweet(){
		return queue.poll();
	}
	
	public Integer getSendQty(){
		return sendQty.poll();
	}
	public Integer getQueueSize(){
		return queue.size();
	}
}
