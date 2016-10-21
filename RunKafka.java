package main;
 import java.util.*;
public class RunKafka {

	public static void main(String[] args) {

		KafkaConsumerBuilder consumer = new KafkaConsumerBuilder();
		consumer.createConsumer("java_test");
	}

}
