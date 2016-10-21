package main;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducerBuilder {

	public static void main(String[] args) throws IOException {

		System.setProperty("java.security.krb5.realm", "CIB.NET");
		//System.setProperty("java.security.krb5.kdc", "swpifrdccib02.fr.intranet:swpifrdccib26.fr.intranet");
		System.setProperty("java.security.auth.login.config","/export/home/adeagproc/adeagproc/kafka_jaas.conf");
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");

		Properties props = new Properties();
		
		props.put("bootstrap.servers", "sldifrdwbhn01.fr.intranet:6667");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.kerberos.service.name", "test_kafka");	
		
		KafkaProducer <String, String> producer = new KafkaProducer<>(props);
		/*for (int i = 0; i < 10; i++){
            producer.send(new KeyedMessage<String, String>(topic, "Test Date: " + new Date()));
        } */
		producer.send(new ProducerRecord<String, String>("topic-eag-fxticks", "This is a dummy message","ssssss"));
		producer.close();
	
	
		
	}
	
}
