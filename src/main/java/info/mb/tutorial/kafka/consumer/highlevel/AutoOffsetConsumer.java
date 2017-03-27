package info.mb.tutorial.kafka.consumer.highlevel;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
/**
 *  
 * @author MBansal
 */
public class AutoOffsetConsumer {
	public static void main(String... s) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "Group1");
		// AUTO OFFSET CONTROL
		props.put("enable.auto.commit", true);
		props.put("auto.commit.interval.ms", 1000);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Consumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("testTopic"));
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("Offset: %d, key: %s, value: %s, Timestamp: %d%n", record.offset(), record.key(),
							record.value(), record.timestamp());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			consumer.close();
		}
	}
}
