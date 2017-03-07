package info.mb.tutorial.spark.stream;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * This is an example class which demonstrates the word count example using Spark
 * Streaming API. 
 *  
 * @author MBansal
 */
public class JavaNetworkWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String... s) throws Exception {
		if (s.length < 2) {
			System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
			System.exit(1);
		}

		/*
		 * CAVEAT- Use more than 2 cores(thread) in master while working on
		 * machines with single core because one thread cannot handle both,
		 * processing and printing of results together.
		 */
		SparkConf conf = new SparkConf().setAppName("Simple Spark Streaming app").setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

		// Create a DStream that will connect to hostname:port, like
		// localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(s[0], Integer.parseInt(s[1]),
				StorageLevel.MEMORY_AND_DISK_SER());

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(SPACE.split(x)).iterator();
			}
		});
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// CORRESSPONDING LAMBDA
		// JavaPairDStream<String, Integer> wordCounts = lines.flatMap(x ->
		// Arrays.asList(SPACE.split(x)).iterator())
		// .mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y)
		// -> x + y);

		// Print the first ten elements of each RDD generated in this DStream to
		// the console
		wordCounts.print();

		jssc.start();
		jssc.awaitTermination();
	}
}
