/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package my.learning.kafka.advanced;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author uditarko
 */
public class TwitterProducer {

    static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private static final String consumerKey = "m4S3u64dXh0SN1QWSZTIVQiit";
    private static final String consumerSecret = "e5xsGFjh29L0wzFRYJ846HrHWDxdoQHxMLJJ8r0S1XlxvYaHWY";
    private static final String token = "251972831-zSs0KCkHSNwgBWnY2NXboZLjFTSKYLbRFrHjz0tm";
    private static final String secret = "y5rOoM6kB3hrW8Nk7ieNImi4pxtmRZccD2LlE5LyhvjRf";
    /**
     * Set up your blocking queues: Be sure to size these properly based on
     * expected TPS of your stream
     */
    private static final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
    private final CountDownLatch latch;
    private final KafkaProducer kafkaProducer;
    private final Client client;

    public TwitterProducer(CountDownLatch latch) {
        this.latch = latch;
        this.client = createTwitterClient();
        this.kafkaProducer = createKafkaProducer();
    }

    public static Properties createProperties() {
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return kafkaProducerProps;
    }

    public static Client createTwitterClient() {

        /**
         * Declare the host you want to connect to, the endpoint, and
         * authentication (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("twitter", "api");

        hosebirdEndpoint.trackTerms(terms);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }

    private static KafkaProducer createKafkaProducer() {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(createProperties());
        return kafkaProducer;
    }

    public void startProcessingMsg() {

        //ProducerRunnable runnable = new ProducerRunnable(this.kafkaProducer, this.client);       
        ProducerRunnable runnable = new ProducerRunnable();
        Thread thread = new Thread(runnable);
        thread.start();
    }

    public void stopProcessingMsg() {
        logger.info("Stop processing called");
        client.stop();
        kafkaProducer.close();
        latch.countDown();
        logger.info("Stopped processing");
    }

    public static void main(String[] args) {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            TwitterProducer twitterProducer = new TwitterProducer(latch);
            twitterProducer.startProcessingMsg();
            ExecutorService consoleReadService = Executors.newSingleThreadExecutor();
            consoleReadService.execute(() -> {
                Scanner scanner = new Scanner(System.in);
                while (latch.getCount() > 0) {
                    if (scanner.hasNextLine()) {
                        if (scanner.nextLine().toLowerCase().contains("exit")) {
                            scanner.close();
                            //call to end the program
                            twitterProducer.stopProcessingMsg();
                        }
                    }
                }

            });
            logger.info("console reading is terminated : " + consoleReadService.isTerminated());
            logger.info("console reading is shutdown : " + consoleReadService.isShutdown());
            consoleReadService.shutdownNow();
            consoleReadService.awaitTermination(5, TimeUnit.MINUTES);
            logger.info("console reading is shutdown : " + consoleReadService.isShutdown());
            logger.info("console reading is terminated : " + consoleReadService.isTerminated());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down application");
                consoleReadService.shutdownNow();
                twitterProducer.stopProcessingMsg();
            }));
            latch.await();
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(TwitterProducer.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    class ProducerRunnable implements Runnable {
        @Override
        public void run() {
            client.connect();
            while (!client.isDone()) {
                try {
                    String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                    logger.info(msg);
                    if (msg != null) {
                        ProducerRecord<String, String> record = new ProducerRecord<>("twitter_topic", null, msg);
                        kafkaProducer.send(record, (rm, e) -> {
                            if (e == null) {
                                logger.info(rm.topic() + " : " + rm.partition() + " : " + rm.offset() + " : " + rm.timestamp());
                            } else {
                                logger.error("There is an issue in inserting the record to the topic : " + e);
                            }
                        });
                    }
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(TwitterProducer.class.getName()).log(Level.SEVERE, null, ex);
                    client.stop();
                    kafkaProducer.close();
                    latch.countDown();
                }
            }
        }
    }
}
