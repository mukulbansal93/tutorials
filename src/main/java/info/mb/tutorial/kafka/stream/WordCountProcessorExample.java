package info.mb.tutorial.kafka.stream;

import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * 
 * @author MBansal
 */
public class WordCountProcessorExample {

	private static class MyProcessor implements Processor<String, String> {

		private ProcessorContext context;
		private KeyValueStore<String, Integer> kvStore;

		@SuppressWarnings("unchecked")
		@Override
		public void init(ProcessorContext context) {
			this.context = context;
			this.context.schedule(1000);
			this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("STORE1");
		}

		@Override
		public void process(String dummy, String line) {

			String[] words = line.toLowerCase(Locale.getDefault()).split(" ");

			for (String word : words) {
				Integer oldValue = kvStore.get(word);
				if (oldValue == null) {
					kvStore.put(word, 1);
				} else {
					kvStore.put(word, oldValue + 1);
				}
			}
		}

		@Override
		public void punctuate(long arg0) {
			KeyValueIterator<String, Integer> kvIterator = this.kvStore.all();
			while (kvIterator.hasNext()) {
				KeyValue<String, Integer> entry = kvIterator.next();
				//System.out.println("[" + entry.key + ", " + entry.value + "]");
				context.forward(entry.key, entry.value.toString());
			}
			kvIterator.close();
			context.commit();
		}

		@Override
		public void close() {

		}
	}

	public static void main(String... s) {

		final String INPUT_TOPIC_NAME = "streams-file-input";
		final String OUTPUT_TOPIC_NAME = "streams-wordcount-output";

		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		// setting offset reset to earliest so that we can re-run the demo code with the
		// same pre-loaded data
		//properties.put(ConsumerConfig.AutoOffsetReset(), "earliest");

		TopologyBuilder builder = new TopologyBuilder();
		builder.addSource("SOURCE", INPUT_TOPIC_NAME);
		builder.addProcessor("PROCESSOR1", MyProcessor::new, "SOURCE");
		builder.addStateStore(Stores.create("STORE1").withStringKeys().withIntegerValues().inMemory().build(),
				"PROCESSOR1");
		builder.addSink("SINK", OUTPUT_TOPIC_NAME, "PROCESSOR1");

		KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);
		kafkaStreams.start();

		// usually the stream application would be running forever,
		// in this example we just let it run for some time and stop since the input
		// data is finite.
		try {
			Thread.sleep(50000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		kafkaStreams.close();

	}

}
