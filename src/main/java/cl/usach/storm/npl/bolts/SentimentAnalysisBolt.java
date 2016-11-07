package cl.usach.storm.npl.bolts;

import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.Status;

public class SentimentAnalysisBolt extends BaseRichBolt {
	
	StanfordCoreNLP pipeline = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        
        this.pipeline = new StanfordCoreNLP(props);
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String line= (String) input.getValueByField("text");
		String user= (String) input.getValueByField("user");
		String lang= (String) input.getValueByField("lang");
		

        int mainSentiment = 0;
        if (line != null && line.length() > 0) {
            int longest = 0;
                       
            Annotation annotation = this.pipeline.process(line);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            	
                Tree tree = sentence.get(SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
 
            }
        }
        System.out.println("@"+user+": "+line+": "+identifySentiment(mainSentiment));


	}
	
	private String identifySentiment(int mainSentiment){
		String sentiment ="";
		switch (mainSentiment) {
		case 0:
			sentiment = "Very Negative";
			break;
		case 1:
			sentiment = "Negative";
			break;
		case 2:
			sentiment = "Neutral";
			break;
		case 3:
			sentiment = "Positive";
			break;
		case 4:
			sentiment = "Very Positive";
			break;
		default:
			sentiment = "Unknow";
			break;
		}
		return sentiment;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
