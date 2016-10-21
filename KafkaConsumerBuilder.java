package main;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Component;

import java.util.Properties;
//import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;


public class KafkaConsumerBuilder {

	  private static final Logger logger = LogManager.getLogger();

	    @Value("#{jcommanderProperties['jaasConfFile']}")
	    private String jaasConfFile;
	   // @Value("#{jcommanderProperties['kdc'] ?: 'swpifrdccib02.fr.intranet:swpifrdccib26.fr.intranet'}")
	    private String kerberosKdc;
	    @Value("#{jcommanderProperties['kdc'] ?: 'CIB.NET'}")
	    private String kerberosRealm;
	    @Value("#{jcommanderProperties['kafkaBootstrapServers'] ?: 'sldifrdwbhn01.fr.intranet:6667'}")
	    private String kafkaBootstrapServers;
	    @Value("#{jcommanderProperties['kafkaSaslKerberosServiceName'] ?: 'kafka'}")
	    private String kafkaSaslKerberosServiceName;
	    @Value("#{jcommanderProperties['kafkaSecurityProtocol'] ?: 'SASL_PLAINTEXT'}")
	    private String kafkaSecurityProtocol;

	    /**
	     * Returns a Kafka consumer that has suscribed to the given topic.
	     * The caller of this method must not forget to close the {@link KafkaConsumer} once it doesn't need it anymore
	     * @param topic the topic to subscribe to
	     * @return the {@link KafkaConsumer}
	     */
	    public KafkaConsumer<String, String> createConsumer(String topic) {
	        logger.info("Start to create a Kafka consumer for topic {}", topic);
	        System.setProperty("java.security.krb5.realm", "CIB.NET");
	        System.setProperty("java.security.krb5.kdc", "swpifrdccib02.fr.intranet:swpifrdccib26.fr.intranet");
	        System.setProperty("java.security.auth.login.config", "C:\\TFS\\HADOOP\\workspacespark\\Producer_Kafka\\kafka_jaas.conf");
	        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");

	        Properties props = new Properties();

	        
	        props.put(BOOTSTRAP_SERVERS_CONFIG, "sldifrdwbhn01.fr.intranet:6667");
	        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	        props.put("security.protocol", "SASL_PLAINTEXT");
	        props.put("sasl.kerberos.service.name", "kafka");
	        props.put(GROUP_ID_CONFIG, "group1");
	        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

	        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	        consumer.subscribe(newArrayList(topic));
	        
	        logger.info("Finished to create a Kafka consumer for topic {}", topic);
	        return consumer;
	    }
	
}
