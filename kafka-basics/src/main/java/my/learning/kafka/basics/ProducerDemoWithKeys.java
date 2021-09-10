/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package my.learning.kafka.basics;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author uditarko
 */
public class ProducerDemoWithKeys {

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        System.out.println("my.learning.kafka.basics.ProducerDemo.main() says hi");

        // Essential Kafka properties
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating the actual producer and sending data
        KafkaProducer<String, String> producer = new KafkaProducer(kafkaProducerProps);

        String topic="firstTopic";
        // Creating Producer Record to send
        for (int i = 0; i < 10; i++) {
            String key="KEY"+i;
            logger.info("Key value :"+key+"\n");
            String value="Hi all from ProducerDemoWithKeys at :" + System.currentTimeMillis();
            ProducerRecord<String, String> record = new ProducerRecord("firstTopic",key,value );

            //Creating producer callback
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata rm, Exception e) {
                    if (e == null) {
                        logger.info("Received new RecordMetadata :" + "\n"                       
                                + "Partition : " + rm.partition() + "\n");
                    }
                }
            }).get();//this is done to make the behaviour of send() synchronous.. not recomended
        }
        //Since send is async and will be carried out in the background in order to ensure message is sent before program execution ends
        producer.flush();
        producer.close();
    }
}
