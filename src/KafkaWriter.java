import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to write messages to Kafka topic
 * @author akotopou
 *
 */
public class KafkaWriter {

    private static Producer<String,String> producer;
    private String password;
    private String user;
    private String bootstrap;
    private String topic;
    private boolean debugOutput;


     
    public KafkaWriter(String bootstrap, String user, String password, String topic, boolean debugOutput) {
    	this.topic = topic;
    	this.bootstrap = bootstrap;
    	this.user = user;
    	this.password = password;
    	this.debugOutput = debugOutput;
    	
    	
         Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        if (user !=null && user.length()>0) {
        	producerProperties.put("security.protocol", "SASL_SSL");
        	producerProperties.put("sasl.mechanism", "PLAIN");
        	producerProperties.put("sasl.jaas.config", PlainLoginModule.class.getName() + " required username=\"" + user + "\" password=\"" + password + "\";");
        }


        producer = new KafkaProducer<String,String>(producerProperties);    	
    }
    
    /**
     * Write single message to Kafka topic
     * @param value
     * @param valueAsKey if true, value is used as key
     */
    public void writeMessage(String value, boolean valueAsKey) {
    	
    	        String key = null;
    	        if (valueAsKey)
    	        	key = value;
    	
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record);
                if (debugOutput) System.out.println("Records written: 1");
    }    
    
    

}


