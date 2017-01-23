package storm.starter;

/**
 * Created by vinay on 12/31/16.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;

import backtype.storm.topology.TopologyBuilder;

import backtype.storm.tuple.Fields;
import storm.starter.bolt.*;
import storm.starter.spout.TweetSpoutSandP;
import storm.starter.spout.TweetSpoutStock;
import storm.starter.spout.YahooSpout;

import java.io.*;
import java.util.ArrayList;

public class Twittertopology {
    public static void main(String[] args) throws InterruptedException, IOException {
        String consumerKey = "qTi3yL0tJisVsLgMh6SWRsml2";
        String consumerSecret = "obeVTlsztO65j9w9WKREIJDGVyK2GcfcpwiSuI4nuUTK7ApoSr";
        String accessToken = "814393855223025664-56ug0wA9TafjlF0HGsn4JJGZx34eM07";
        String accessTokenSecret = "Y1YFRHkBCKoL8rLCOwx2BvXKhrMU0qZjfjtvW5oOHhqdx";
        Twittertopology twittertopology= new Twittertopology();
        ClassLoader classLoader=twittertopology.getClass().getClassLoader();
        File file = new File(classLoader.getResource("Keywords.txt").getFile());
        BufferedReader bufferedReader=new BufferedReader(new FileReader(file));
        ArrayList<String> list=new ArrayList<String>();
        String line="";
        while ((line=bufferedReader.readLine())!=null){
            list.add(line);
        }
        String[]keywords_topcompanies=list.toArray(new String[0]);
        list.clear();
        file= new File(classLoader.getResource("Stocks.txt").getFile());
        bufferedReader=new BufferedReader(new FileReader(file));
        while ((line=bufferedReader.readLine())!=null){
            list.add(line);
        }

        String[] keyWords_SandP=list.toArray(new String[0]);


        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("keyword-spout", new TweetSpoutStock(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keywords_topcompanies),1);
        builder.setSpout("sandp-spout", new TweetSpoutSandP(consumerKey,
                        consumerSecret, accessToken, accessTokenSecret, keyWords_SandP),1);
        builder.setSpout("stockvalue-spout", new YahooSpout(),6);
        builder.setBolt("sentiment-bolt", new SentimentAnalysisBolt(), 8).shuffleGrouping("sandp-spout").shuffleGrouping("keyword-spout");
        builder.setBolt("parsetweet-bolt", new ParseTweetBolt(), 8).shuffleGrouping("sandp-spout").shuffleGrouping("keyword-spout");

        builder.setBolt("redispublishword-bolt", new RedisPublishbolt(),1).globalGrouping("parsetweet-bolt");
        builder.setBolt("aggregator-bolt",new AggregatorBolt(),3).fieldsGrouping("sentiment-bolt",new Fields("companyname")).fieldsGrouping("stockvalue-spout",new Fields("companyname"));
        builder.setBolt("predictor-bolt",new LinearRegress(),1).globalGrouping("aggregator-bolt");
       builder.setBolt("reportprediction-bolt", new ReportPrediction(),1).globalGrouping("predictor-bolt");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
        //Thread.sleep(100000);
        //cluster.shutdown();
    }
}
