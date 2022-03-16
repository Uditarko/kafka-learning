/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package my.learning.kafka.consumer.realworld;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

/**
 *
 * @author uditarko
 */
public class IdempotentKafkaTwitterConsumerManualCommit {

    static final org.slf4j.Logger logger = LoggerFactory.getLogger(IdempotentKafkaTwitterConsumerManualCommit.class);
    static final String topicName = "twitter_bulk_topic";
    static final String indexName = "twitter_bulk_req";
    final RestHighLevelClient client;
    final CountDownLatch latch;
    static volatile boolean active = true;

    public IdempotentKafkaTwitterConsumerManualCommit(CountDownLatch latch) {
        client = createElasticClient();
        this.latch = latch;
    }

    private static RestHighLevelClient createElasticClient() {
        String host = "localhost";
        RestHighLevelClient elasticClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));//.builder(new HttpHost(host, 9200, "http")).build();
        return elasticClient;
    }

    //Creating consumer properties
    private static KafkaConsumer createConsumer() {
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "IdempotentKafkaTwitterConsumerManualCommit-1");
        kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaConsumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        //Creating the KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        consumer.subscribe(Collections.singleton(topicName));
        return consumer;
    }

    public void startProcessing() {
        Processor prcsr = new Processor(this.client, createConsumer());
        Thread thread = new Thread(prcsr);
        thread.start();

    }

    public void stopProcessing() {
        try {
            active=false;
            client.close();
        } catch (IOException ex) {
            logger.error("Failure caused by : "+ex);
        }
        finally{
            latch.countDown();
        }
    }

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        IdempotentKafkaTwitterConsumerManualCommit consm2 = new IdempotentKafkaTwitterConsumerManualCommit(latch);
        consm2.startProcessing();
        ExecutorService consoleService = Executors.newSingleThreadExecutor();
        consoleService.execute(() -> {
            Scanner scanner = new Scanner(System.in);
            while (latch.getCount() > 0) {
                if (scanner.hasNextLine()) {
                    if (scanner.nextLine().toLowerCase().contains("exit")) {
                        scanner.close();
                        //call to end the program
                        consm2.stopProcessing();
                    }
                }
            }
        });
        try {
            latch.await();
            consoleService.shutdownNow();
            consoleService.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            Logger.getLogger(IdempotentKafkaTwitterConsumerManualCommit.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    class Processor implements Runnable {

        final org.slf4j.Logger plogger = LoggerFactory.getLogger(Processor.class);
        final RestHighLevelClient elasticClient;
        final KafkaConsumer consumer;

        Processor(RestHighLevelClient elasticClient, KafkaConsumer consumer) {
            this.elasticClient = elasticClient;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            plogger.info("Count is :: ",latch.getCount());
            while (active) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (records.count() > 0) {
                    BulkRequest req = new BulkRequest();
                    for (ConsumerRecord<String, String> record : records) {
                        plogger.info("Key : " + record.key() + " || " + "value : " + record.value());
                        plogger.info("Partition : " + record.partition() + " || " + "offset : " + record.offset());                        
                        req.add(new IndexRequest(indexName).id(record.key()).source(record.value(), XContentType.JSON));
                    }
                    try {
                        BulkResponse response = elasticClient.bulk(req, RequestOptions.DEFAULT);
                        BulkItemResponse[] resArr = response.getItems();
                        for (BulkItemResponse re : resArr) {
                            if (re.isFailed()) {
                                plogger.error("failure in processing elastic request " + re.getId() + " : " + re.getIndex() + " : " 
                                        + re.getItemId() + " : " + re.getFailureMessage());
                            }
                        }
                    } catch (IOException ex) {
                        plogger.error("failure in processing elastic request - Key : ", ex);
                    }
                    finally{
                        consumer.commitSync();
                    }
                }
            }
            consumer.close(Duration.ofMillis(5000));
        }
    }
}
