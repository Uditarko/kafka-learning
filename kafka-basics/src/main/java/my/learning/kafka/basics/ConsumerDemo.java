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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author uditarko
 */
public class ConsumerDemo {
    
    static final Logger logger=LoggerFactory.getLogger(ConsumerDemo.class);
    static final String topicName="firstTopic";
    
    public static void main(String[] args) {
        //Creating consumer properties
        Properties kafkaConsumerProperties=new Properties();
        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "JavaConsumer");
        kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        //Creating the KafkaConsumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(kafkaConsumerProperties);
        consumer.subscribe(Collections.singleton(topicName));
        
        //reading the actual records in to ConsumerRecord
        while(true){
            ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records)
            {
                logger.info("Key : "+record.key()+" || "+"value : "+record.value());
                logger.info("Partition : "+record.partition()+" || "+"offset : "+record.offset());
            }
        }
    }
}
