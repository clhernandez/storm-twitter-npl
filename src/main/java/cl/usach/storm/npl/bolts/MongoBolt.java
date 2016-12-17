package cl.usach.storm.npl.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoBolt extends BaseRichBolt{

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String text= (String) input.getValueByField("text");
		String user= (String) input.getValueByField("user");
		String lang= (String) input.getValueByField("lang");
		String sentiment = (String) input.getValueByField("sentiment");
		
		try(MongoClient mongoClient = new MongoClient()){

			MongoDatabase db = mongoClient.getDatabase("storm-twitter-npl");
			
			Document doc = new Document();
			doc.append("user", user);
			doc.append("text", text);
			doc.append("lang", lang);
			doc.append("sentiment", sentiment);
					
			db.getCollection("sentiment").insertOne(doc);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
