package info.mb.tutorial.kafka.producer;

import java.util.Properties;

import org.apache.hadoop.util.Time;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {
	public static void main(String... s) {

		final String TOPIC = "test-topic-from-code";
		final Integer PARTITION = 0;
		final Long TIMESTAMP = Time.now();

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("ack", "all");
		props.put("retries", 0);
		props.put("batch-size", 100);
		props.put("linger.ms", 5);
		props.put("buffer.memeory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 1; i <= 1000; i++) {
			producer.send(new ProducerRecord<String, String>(TOPIC, PARTITION, TIMESTAMP, Integer.toString(i),
					Integer.toString(i)));
		}
		producer.close();
	}
}
