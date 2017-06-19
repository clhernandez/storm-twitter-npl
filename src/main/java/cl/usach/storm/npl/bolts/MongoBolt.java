package cl.usach.storm.npl.bolts;

import java.text.SimpleDateFormat;
import java.util.Date;
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
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {}

	@Override
	public void execute(Tuple input) {
		String text= (String) input.getValueByField("text");
		String user= (String) input.getValueByField("user");
		String lang= (String) input.getValueByField("lang");
		String sentiment = (String) input.getValueByField("sentiment");
		
		try(MongoClient mongoClient = new MongoClient()){

			MongoDatabase db = mongoClient.getDatabase("storm-twitter-npl");
			SimpleDateFormat dt1 = new SimpleDateFormat("yyyyMMddHHmmss");
			
			Document doc = new Document();
			doc.append("user", user);
			doc.append("text", text);
			doc.append("lang", lang);
			doc.append("sentiment", sentiment);
			doc.append("timestamp", dt1.format(new Date()));
					
			db.getCollection("sentiment").insertOne(doc);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
