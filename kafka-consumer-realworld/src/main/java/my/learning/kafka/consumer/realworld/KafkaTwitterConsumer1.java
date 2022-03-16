/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package my.learning.kafka.consumer.realworld;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.slf4j.LoggerFactory;

/**
 *
 * @author uditarko
 */
public class KafkaTwitterConsumer1 {

    static final org.slf4j.Logger kafkaTwitterConsumer1Logger = LoggerFactory.getLogger(KafkaConsumerWrapper.class);
    //static final JsonParser parser = new JsonParser();
    static final String topicName = "twitter_topic";

    public static class ElasticClientWrapper {

        public RestClient createElasticClient() {
            String host = "localhost";
            RestClient elasticClient = RestClient.builder(new HttpHost(host, 9200, "http")).build();
            return elasticClient;
        }
    }

    public static class KafkaConsumerWrapper {

        //Creating consumer properties
        public static KafkaConsumer createConsumer() {
            Properties kafkaConsumerProperties = new Properties();
            kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "TestGroup");
            kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Creating the KafkaConsumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerProperties);
            return consumer;
        }
    }

    public static void main(String[] args) {
        ElasticClientWrapper eWrap = new ElasticClientWrapper();
        RestClient elasticClient = eWrap.createElasticClient();

        try {
            KafkaConsumer consumer = KafkaConsumerWrapper.createConsumer();
            consumer.subscribe(Collections.singleton(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    kafkaTwitterConsumer1Logger.info("Key : " + record.key() + " || " + "value : " + record.value());
                    kafkaTwitterConsumer1Logger.info("Partition : " + record.partition() + " || " + "offset : " + record.offset());
                    if (record.value() != null) {
                        Request req = new Request("PUT", "/twitter/_doc/" + record.key());
                        //obj.remove("id");
                        req.setJsonEntity(record.value());
                        elasticClient.performRequest(req);
                    }
                }
            }
        } catch (Throwable ex) {
            Logger.getLogger(KafkaTwitterConsumer1.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                elasticClient.close();
            } catch (IOException ex) {
                Logger.getLogger(KafkaTwitterConsumer1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
