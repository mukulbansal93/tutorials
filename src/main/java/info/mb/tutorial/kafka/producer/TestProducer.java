package info.mb.tutorial.kafka.producer;

import java.util.Properties;

import org.apache.hadoop.util.Time;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 
 * @author MBansal
 */
public class TestProducer {
	public static void main(String... s) {

		if (s.length < 3) {
			System.err.println("Usage: TestProducer <topic-name> <bootstrap-servers> <partition>");
			System.exit(1);
		}

		final String TOPIC = s[0];
		final String BOOTSTRAP_SERVERS = s[1];
		final Integer PARTITION = Integer.parseInt(s[2]);

		Producer<String,String> KAFKA_PRODUCER=initKafkaProducer(BOOTSTRAP_SERVERS);
		for (int i = 1; i <= 1000; i++) {
			pushToKafka(KAFKA_PRODUCER, TOPIC, PARTITION, Integer.toString(i), Integer.toString(i));
		}
		KAFKA_PRODUCER.close();
	}

	public static Producer<String, String> initKafkaProducer(String bootstrapServers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("ack", "all");
		props.put("retries", 0);
		props.put("batch-size", 100);
		props.put("linger.ms", 5);
		props.put("buffer.memeory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer<>(props);
	}

	public static void pushToKafka(Producer<String, String> producer, String topic, Integer partition, String key, String value) {
		producer.send(new ProducerRecord<String, String>(topic, partition, Time.now(), key, value));
	}

	public static void pushToKafka(Producer<String, String> producer, String topic, String value) {
		producer.send(new ProducerRecord<String, String>(topic, value));
	}
}
