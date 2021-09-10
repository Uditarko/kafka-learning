/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package my.learning.kafka.basics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author uditarko
 */
public class ConsumerDemoWithSeekAndAssign {
    
    static final Logger logger=LoggerFactory.getLogger(ConsumerDemoWithSeekAndAssign.class);
    static final String topicName="firstTopic";
    
    public static void main(String[] args) {
        //Creating consumer properties
        Properties kafkaConsumerProperties=new Properties();
        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        //Creating the KafkaConsumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(kafkaConsumerProperties);
        TopicPartition tp= new TopicPartition("firstTopic",0);
        
        consumer.assign(Arrays.asList(tp));
        Long offset=consumer.endOffsets(Arrays.asList(tp)).get(tp)-10L;
        consumer.seek(tp,offset);
        //reading the actual records in to ConsumerRecord
        int i=0;
        while(i<10){
            ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records)
            {
                logger.info("Key : "+record.key()+" || "+"value : "+record.value());
                logger.info("Partition : "+record.partition()+" || "+"offset : "+record.offset());
            }
            i++;
        }
    }
}
