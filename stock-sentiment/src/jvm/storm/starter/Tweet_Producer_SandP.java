package storm.starter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by vinay on 6/11/17.
 */
public class Tweet_Producer_SandP {
    Producer<String,String> tweet_producer;
    String[] keywords_SandP;
    public Tweet_Producer_SandP(Properties properties) throws IOException {
        tweet_producer = new KafkaProducer<String, String>(properties);
        ClassLoader classLoader=this.getClass().getClassLoader();
        File file = new File(classLoader.getResource("Stocks.txt").getFile());
        BufferedReader bufferedReader=new BufferedReader(new FileReader(file));
        ArrayList<String> list=new ArrayList<String>();
        String line="";
        while ((line=bufferedReader.readLine())!=null){
            list.add(line);
        }
        keywords_SandP=list.toArray(new String[0]);
        list.clear();
    }

    public void run(){
        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("sandp",status.getText());
                tweet_producer.send(producerRecord);
                //System.out.println(status.getText());
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
            public void onException(Exception ex) {
            }

            @Override
            public void onStallWarning(StallWarning arg0) {

            }
        };
        TwitterStream twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(Tweet_Constants.CONSUMER_KEY, Tweet_Constants.CONSUMER_SECRET);
        AccessToken token = new AccessToken(Tweet_Constants.ACCESS_TOKEN, Tweet_Constants.ACCESS_TOKEN_SECRET);
        twitterStream.setOAuthAccessToken(token);
        FilterQuery query = new FilterQuery().track(keywords_SandP);
        String[] lang = { "en" };
        query.language(lang);
        twitterStream.filter(query);

    }
}
