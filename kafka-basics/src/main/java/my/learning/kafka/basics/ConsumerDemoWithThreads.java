/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package my.learning.kafka.basics;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author uditarko
 */
public class ConsumerDemoWithThreads {

    static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
    static final String topicName = "firstTopic";
    static final String broker = "localhost:9092";
    static final String offsetReset = "earliest";
    static final String groupId = "ConsumerDemoWithThreads3";
    public final CountDownLatch latch;
    public final KafkaConsumer<String, String> consumer;

    public ConsumerDemoWithThreads(CountDownLatch latch) {
        this.latch = latch;
        this.consumer = createConsumer();
    }

    private KafkaConsumer<String, String> createConsumer() {
        //Creating consumer properties
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

        //Creating the KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        consumer.subscribe(Collections.singleton(topicName));
        return consumer;
    }

    public void startConsuming() {
        ConsumerRunnable runnable = new ConsumerRunnable(this.consumer);
        Thread t1 = new Thread(runnable);
        t1.start();
    }

    public void stopConsuming() {
        this.consumer.wakeup();
    }

    public static void main(String[] args) {

        //To start off the thread
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerDemoWithThreads cd = new ConsumerDemoWithThreads(latch);
        cd.startConsuming();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Application is shutting down");
            cd.stopConsuming();
        }));
        try {
            latch.await();
            logger.info("Application is shutting down called");
            System.out.println("Application is shutting down called");
        } catch (InterruptedException ex) {
            logger.info("Stopping application due to exception : " + ex);
            System.out.println("Stopping application due to exception : " + ex);
        } finally {
            logger.info("Application is shutting down");
            System.out.println("my.learning.kafka.basics.ConsumerDemoWithThreads.main() is shutting down");
        }
    }

    class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(KafkaConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            //Start subscribing
            try {
                while (true) {
                    //read the records in this thread
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key : " + record.key() + " || " + "value : " + record.value());
                        logger.info("Partition : " + record.partition() + " || " + "offset : " + record.offset());
                    }
                }
            } //when wakeup is called this exception is thrown
            catch (WakeupException e) {
                logger.info("Shutting down consumer runnable because consumer WakeUp() is called : " + e);
                System.out.println("Shutting down consumer runnable because consumer WakeUp() is called : " + e);
            } finally {
                logger.info("Shutting down consumer");
                System.out.println("Shutting down consumer");
                consumer.close();
                latch.countDown();
            }

        }
    }
}
