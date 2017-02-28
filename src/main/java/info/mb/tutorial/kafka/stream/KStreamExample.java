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

public class KStreamExample {
	public static void main(String... s) {

		final String INPUT_TOPIC_NAME = "streams-file-input";
		final String OUTPUT_TOPIC_NAME = "streams-wordcount-output";

		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

		KStreamBuilder builder = new KStreamBuilder();

		// Serializers/deserializers (serde) for String and Long types
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		// Construct a `KStream` from the input topic ""streams-file-input",
		// where message values
		// represent lines of text (for the sake of this example, we ignore
		// whatever may be stored
		// in the message keys).

		KStream<String, String> streamSource = builder.stream(INPUT_TOPIC_NAME);

		//countByKey() takes parameter- name of the resulting KTable
		KTable<String, Long> kTable = streamSource.flatMapValues((x) -> Arrays.asList(x.toLowerCase().split("\\W+")))
				.map((key, value) -> new KeyValue<String, String>(value, value)).countByKey("kTable");

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
