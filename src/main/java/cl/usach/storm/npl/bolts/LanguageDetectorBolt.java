package cl.usach.storm.npl.bolts;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import cl.usach.storm.npl.eda.Tweet;
import twitter4j.Status;

public class LanguageDetectorBolt extends BaseRichBolt {
	private List<LanguageProfile> languageProfiles;
	private LanguageDetector languageDetector = null;
	private TextObjectFactory textObjectFactory = null;
	private OutputCollector collector;
		
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		// TODO Auto-generated method stub
		try {
			languageProfiles = new LanguageProfileReader().readAllBuiltIn();
			languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
			        .withProfiles(languageProfiles)
			        .build();
			textObjectFactory = CommonTextObjectFactories.forDetectingShortCleanText();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		String line="";
		String user="";
		if(input.getValueByField("tweet") instanceof Status){
			Status tweet = (Status) input.getValueByField("tweet");
			line = tweet.getText();
			user = tweet.getUser().getScreenName();
		}else if(input.getValueByField("tweet") instanceof Tweet){
			Tweet tweet = (Tweet) input.getValueByField("tweet");
			line = tweet.getText();
			user = tweet.getUser();
		}
		
		
		try {
			
			TextObject textObject = textObjectFactory.forText(line);
			Optional<LdLocale> lang = languageDetector.detect(textObject);
			String textlang = "";
			if(lang.isPresent() ){
				//System.out.println(line+" >> "+lang.get().getLanguage());
				textlang = lang.get().getLanguage().toUpperCase();
			}
			collector.emit(new Values(user, textlang , line));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "lang", "text"));
	}

}
