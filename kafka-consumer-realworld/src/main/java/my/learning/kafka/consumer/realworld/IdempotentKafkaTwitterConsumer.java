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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.slf4j.LoggerFactory;

/**
 *
 * @author uditarko
 */
public class IdempotentKafkaTwitterConsumer {

    static final org.slf4j.Logger kafkaTwitterConsumer1Logger = LoggerFactory.getLogger(IdempotentKafkaTwitterConsumer.class);
    static final String topicName = "twitter_topic";

    final RestClient restClient;
    final CountDownLatch latch;
    volatile boolean active=true;

    public IdempotentKafkaTwitterConsumer(CountDownLatch latch) {
        restClient = createElasticClient();
        this.latch = latch;
    }

    private static RestClient createElasticClient() {
        String host = "localhost";
        RestClient elasticClient = RestClient.builder(new HttpHost(host, 9200, "http")).build();
        return elasticClient;
    }

    //Creating consumer properties
    private static KafkaConsumer createConsumer() {
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "IdempotentKafkaTwitterConsumer-1");
        kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Creating the KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        consumer.subscribe(Collections.singleton(topicName));
        return consumer;
    }

    public void startProcessing() {
        Processor prcsr = new Processor(this.restClient, createConsumer());
        Thread thread = new Thread(prcsr);
        thread.start();

    }

    public void stopProcessing() {
        try {
            restClient.close();
        } catch (IOException ex) {
            Logger.getLogger(IdempotentKafkaTwitterConsumer.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            latch.countDown();
        }
    }

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        IdempotentKafkaTwitterConsumer consm2 = new IdempotentKafkaTwitterConsumer(latch);
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
        consoleService.shutdownNow();
        try {
            consoleService.awaitTermination(5, TimeUnit.MINUTES);
            latch.await();
        } catch (InterruptedException ex) {
            Logger.getLogger(IdempotentKafkaTwitterConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    class Processor implements Runnable {

        final RestClient elasticClient;
        final KafkaConsumer consumer;

        Processor(RestClient elasticClient, KafkaConsumer consumer) {
            this.elasticClient = elasticClient;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            while (elasticClient.isRunning()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    kafkaTwitterConsumer1Logger.info("Key : " + record.key() + " || " + "value : " + record.value());
                    kafkaTwitterConsumer1Logger.info("Partition : " + record.partition() + " || " + "offset : " + record.offset());
                    if (record.value() != null) {
                        Request req = new Request("PUT", "/twitter_idempotent/_doc/" + record.key());
                        req.setJsonEntity(record.value());
                        try {
                            elasticClient.performRequest(req);
                        } catch (IOException ex) {
                            Logger.getLogger(IdempotentKafkaTwitterConsumer.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            }
            consumer.close(Duration.ofMillis(5000));
        }
    }

}
