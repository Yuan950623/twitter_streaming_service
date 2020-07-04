package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSource {

    public static void main(String[] args) throws TwitterException, IOException{

        final LinkedBlockingQueue<Status> feedsQueue = new LinkedBlockingQueue<Status>();

        // set up twitter streaming api credentials
        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("pzuMdvcjCoFq6Bp1MPJRbS7V4")
                .setOAuthConsumerSecret("OP7qBJ7AQwYXjeytc1bA4GaTLb7qH28b3vc5cIw0Z6N7RDFh1C")
                .setOAuthAccessToken("1220483156316512264-nor9pPImZayoJHDxOsR3h1TZOZsL3s")
                .setOAuthAccessTokenSecret("7sNBJzNqfPQ9qvmfQNFPkjnXCfGMIQnzt6CfX8RRlxokl");

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        StatusListener listener = new StatusListener(){

            public void onStatus(Status status) {
                //System.out.println(status.getUser().getName() + " : " + status.getText());
                feedsQueue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        twitterStream.addListener(listener);

        // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        twitterStream.sample();

        // Kafka side --------------------------------------------------------------------------
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10009009; i++) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Status curr_feed = feedsQueue.poll();
            if (curr_feed != null) {
                producer.send(new ProducerRecord<String, String>("first", i + "", "message: " + curr_feed.getText()));
            }

        }

//        for (int i = 0; i < 10000000; i++) {
//            producer.send(new ProducerRecord<String, String>("first", i + "", "message: " + i));
//        }

        producer.close();
    }
}
