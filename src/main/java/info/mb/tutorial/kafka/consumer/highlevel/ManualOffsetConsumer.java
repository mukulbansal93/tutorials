package info.mb.tutorial.kafka.consumer.highlevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 
 * @author MBansal
 */
public class ManualOffsetConsumer {
	public static void main(String... s) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "Group1");
		// MANUAL OFFSET CONTROL
		props.put("enable.auto.commit", "false");
		//props.put("auto.commit.interval.ms", 1000);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("max.poll.records",5);
		props.put("max.poll.interval.ms",30000);

		Consumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("test"));

		final int minBatchSize = 2;
		List<ConsumerRecord<String, String>> consumerRecordsList = new ArrayList<>();
		try {
			while (true) {
				System.out.println("Started Polling...");
				ConsumerRecords<String, String> records = consumer.poll(10);
				for (ConsumerRecord<String, String> record : records) {
					consumerRecordsList.add(record);
				}
				System.out.println("Stopped Polling.");

				// RECORD PROCESSIGN AND OFFSET COMMITITNG
				if (consumerRecordsList.size() > minBatchSize)
					for (ConsumerRecord<String, String> record : consumerRecordsList) {
						processRecord(record);
					}
				consumer.commitSync();
				consumerRecordsList.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}

	private static void processRecord(ConsumerRecord<String, String> record) {
		try {
			System.out.println("Processing.......");
			Thread.sleep(4000);
			System.out.printf("Offset: %d, key: %s, value: %s, Timestamp: %d%n", record.offset(), record.key(),
					record.value(), record.timestamp());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
