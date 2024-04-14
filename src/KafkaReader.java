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
 * Class to read records from Kafka topic
 * @author akotopou
 *
 */
public class KafkaReader {

    private static Consumer<String,String> consumer;
    private String password;
    private String user;
    private String bootstrap;
    private String topic;
    private boolean debugOutput;

     
    public KafkaReader(String bootstrap, String user, String password, String topic, boolean debugOutput) {
    	this.topic = topic;
    	this.bootstrap = bootstrap;
    	this.user = user;
    	this.password = password;
    	this.debugOutput = debugOutput;
    	
    	
         Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProperties.put("retries", 3); // retries on transient errors and load balancing disconnection
        consumerProperties.put("max.request.size", 1024 * 1024); // limit request size to 1MB
        consumerProperties.put("session.timeout.ms", "30000");

        
        if (user !=null && user.length()>0) {
	        consumerProperties.put("security.protocol", "SASL_SSL");
	        consumerProperties.put("sasl.mechanism", "PLAIN");
	        consumerProperties.put("sasl.jaas.config", PlainLoginModule.class.getName() + " required username=\"" + user + "\" password=\"" + password + "\";");
        }
        consumer = new KafkaConsumer<String,String>(consumerProperties);
        consumer.subscribe(Arrays.asList(topic));   	
    	
    }

    /**
     * Poll list of messages from Kafka topic. Return empty list if no messages present.
     * @return
     */

    public List<String> getMessages() {
    	
    	ArrayList<String> messages = new ArrayList<String>();

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        
        if (debugOutput) System.out.println("Records read:" + records.count());

        for (ConsumerRecord<String, String> record : records) {
        	messages.add(record.value());
        }
        
        return messages;
    }
    
    /**
     * Debug operation: Read all available topics from input Kafka
     */
    public void listTopics() {
	    Map<String, List<PartitionInfo>> topics = consumer.listTopics();
	    Set<String> topicNames = topics.keySet();
	    
	    for (String name:topicNames) {
	    	 System.out.println(name);
	    }
    }

}


