/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/ 
 */
package cl.usach.storm.npl.spouts;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterSampleSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;
    private static Logger logger = LoggerFactory.getLogger(TwitterSampleSpout.class);
    
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector){
		
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		Properties twitterProperties = new Properties();
		File twitter4jPropsFile = new File("/home/clhernandez/repos/StormWordUsersCount/config/twitter4j.properties");
		
		if (!twitter4jPropsFile.exists()) {
			logger.error(
					"Cannot find twitter4j.properties file in this location :[{}]",
					twitter4jPropsFile.getAbsolutePath());
			return;
		}

		try {
			twitterProperties.load(new FileInputStream(twitter4jPropsFile));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(twitterProperties.getProperty("oauth.consumerKey"));
		cb.setOAuthConsumerSecret(twitterProperties.getProperty("oauth.consumerSecret"));
		cb.setOAuthAccessToken(twitterProperties.getProperty("oauth.accessToken"));
		cb.setOAuthAccessTokenSecret(twitterProperties.getProperty("oauth.accessTokenSecret"));
		cb.setDebugEnabled(false);
		cb.setPrettyDebugEnabled(false);
		cb.setIncludeMyRetweetEnabled(false);


		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

            @Override
            public void onStallWarning(StallWarning stallWarning) {
            }

            @Override
			public void onException(Exception e) {
			}
		};

        TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
		twitterStream = factory.getInstance();
		twitterStream.addListener(listener);
		
		FilterQuery tweetFilterQuery = new FilterQuery();
		//Filter language spanish
		//double position[][] = {{-70.85495, -33.61927}, {-70.499268, -33.329457}};//Filter only santiago de chile
		//double position[][] = {{-76.376953, -54.984337}, {-69.153442, -18.177515}};//Filter only chile
		double position[][] = {{-130.605469, 30.557531}, {-65.566406, 43.289202}};//Filter only usa
		//tweetFilterQuery.language(new String[]{"en"});
		tweetFilterQuery.locations(position);
		twitterStream.filter(tweetFilterQuery);
		
		//twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(1);
        } else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
