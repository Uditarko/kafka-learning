/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package my.learning.kafka.producer.realworld;

import com.google.common.collect.Lists;
import com.google.gson.JsonParser;
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
public class KafkaRealTwitterProducer {

    static final Logger logger = LoggerFactory.getLogger(KafkaRealTwitterProducer.class.getName());
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
    static final String topicName = "twitter_bulk_topic";
    //static final String topicName = "twitter_topic";
    public KafkaRealTwitterProducer(CountDownLatch latch) {
        this.latch = latch;
        this.client = createTwitterClient();
        this.kafkaProducer = createKafkaProducer();
    }

    public static Properties createProperties() {
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //The fololowing is for making the producer more resilient to failure i.e. a safe producer
        /*this makes producer ask for acknowledgement from all insync replicas further enhanced bys setting higher value for min.insync.replicas property on the broker*/
        kafkaProducerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        /*How many messages can be processed parrallely by the broker
        the safe value for Kafka > 0.11 is 5 ohterwise 1
        It means 5 messages can be processed while a batch is being processed by the broker
        where as 1 means that no ther messages will be processed while processing of a batch/msg is in progress*/
        kafkaProducerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        kafkaProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /*How many retiries will be attempted in the case of a failure, 
        also controlled by delivery.timeout.ms which ever one is reached first
        the retry for the message will stop*/
        kafkaProducerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        /*The 4 properties set baove above are already set if we just enable the below property*/
        kafkaProducerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //Increae throughput and performance
        /*sets the time after which producer will actually send data, either batch.size 
        limit will be reached or linger.ms will be exceeded before sending message*/
        kafkaProducerProps.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        kafkaProducerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);//32kb is the batch.size set
        /*type of compression to be applied on batch lz4 and snappy has the best compression ratio to cpu cycle
        taken gzip is also there compression is good but too much cpu intensive*/
        kafkaProducerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

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
        List<String> terms = Lists.newArrayList("cricket");

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
            KafkaRealTwitterProducer twitterProducer = new KafkaRealTwitterProducer(latch);
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
            java.util.logging.Logger.getLogger(KafkaRealTwitterProducer.class.getName()).log(Level.SEVERE, null, ex);
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
                        String id = getTweetId(msg);
                        if (id != null) {
                            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, id, msg);
                            kafkaProducer.send(record, (rm, e) -> {
                                if (e == null) {
                                    logger.info(rm.topic() + " : " + rm.partition() + " : " + rm.offset() + " : " + rm.timestamp());
                                } else {
                                    logger.error("There is an issue in inserting the record to the topic : " + e);
                                }
                            });
                        }
                    }
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(KafkaRealTwitterProducer.class.getName()).log(Level.SEVERE, null, ex);
                    client.stop();
                    kafkaProducer.close();
                    latch.countDown();
                }
            }
        }
    }

    private String getTweetId(String tweet) {
        String id = null;
        try {
            id = JsonParser.parseString(tweet).getAsJsonObject().get("id_str").getAsString();
        } catch (NullPointerException e) {
            id = JsonParser.parseString(tweet).getAsJsonObject().get("id").getAsString();
        }
        return id;
    }
}
