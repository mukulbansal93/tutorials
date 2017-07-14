package info.mb.tutorial.kafka.stream;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

/**
 * 
 * @author MBansal
 */
public class WordCountKStreamExample {
	public static void main(String... s) {

		final String INPUT_TOPIC_NAME = "streams-file-input";
		final String OUTPUT_TOPIC_NAME = "streams-wordcount-output";

		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);

		KStreamBuilder builder = new KStreamBuilder();

		// Serializers/deserializers (serde) for String and Long types
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		// Construct a `KStream` from the input topic ""streams-file-input",
		// where message values
		// represent lines of text (for the sake of this example, we ignore
		// whatever may be stored
		// in the message keys).

		KStream<String, String> streamSource = builder.stream(stringSerde, stringSerde, INPUT_TOPIC_NAME);

		// count() takes parameter- name of the resulting KTable
		/*
		 * KTable<String, Long> kTable = streamSource.flatMapValues(new
		 * ValueMapper<String, Iterable<String>>() {
		 * 
		 * @Override public Iterable<String> apply(String value) { return
		 * Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")); }
		 * }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
		 * 
		 * @Override public KeyValue<String, String> apply(String key, String value) {
		 * return new KeyValue<>(value, value); } }).groupByKey().count("Counts");
		 */

		// CORRESSPONDING LAMBDA
		KTable<String, Long> kTable = streamSource.flatMapValues((x) -> Arrays.asList(x.toLowerCase().split("\\W+")))
				.map((key, value) -> new KeyValue<String, String>(value, value)).groupByKey().count("Counts");

		// need to override value serde to Long type
		kTable.to(stringSerde, longSerde, OUTPUT_TOPIC_NAME);

		KafkaStreams streams = new KafkaStreams(builder, properties);

		streams.start();

		// usually the stream application would be running forever,
		// in this example we just let it run for some time and stop since the
		// input data is finite.
		try {
			Thread.sleep(50000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		streams.close();
	}
}
