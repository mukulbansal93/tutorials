package info.mb.tutorial.kafka.consumer.highlevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
/**
 *  
 * @author MBansal
 */
public class FineControlManualOffsetConsumer {
	public static void main(String... s) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "Group1");
		// MANUAL OFFSET CONTROL
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.intereval.ms", 1000);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Consumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("testTopic"));

		// FINER MANUAL OFFSET CONTROL CODE
		List<ConsumerRecord<String, String>> consumerRecordsList = new ArrayList<>();
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (TopicPartition partition : records.partitions()) {
					for (ConsumerRecord<String, String> record : records) {
						consumerRecordsList.add(record);
					}
					// RECORD PROCESSIGN AND OFFSET COMMITITNG
					for (ConsumerRecord<String, String> record : consumerRecordsList) {
						System.out.printf("Offset: %d, key: %s, value: %s, Timestamp: %d%n", record.offset(),
								record.key(), record.value(), record.timestamp());

						long lastOffset = record.offset();
						Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
						map.put(partition, new OffsetAndMetadata(lastOffset + 1));
						consumer.commitSync(map);

						// CONCISE SYNTAX OF ABOVE LINES
						// consumer.commitSync(Collections.singletonMap(partition,
						// new OffsetAndMetadata(lastOffset + 1)));
					}
					consumerRecordsList.clear();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
