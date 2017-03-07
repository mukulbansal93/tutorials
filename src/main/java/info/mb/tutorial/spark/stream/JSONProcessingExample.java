
package info.mb.tutorial.spark.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import info.mb.tutorial.kafka.producer.TestProducer;
import info.mb.tutorial.spark.dto.JSONInputDTO;
import info.mb.tutorial.spark.dto.JSONOutputDTO;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * This is an example class which demonstrates JSON Processing using Spark
 * Streaming and Spark SQL API. It reads message from a Kafka topic, processes
 * them and then pushes the processed messages back to Kafka. The input and
 * output formats are JSON with the JSONInputDTO and JSONOutputDTO as their
 * respective DTO classes.
 * 
 * @author MBansal
 */
public class JSONProcessingExample {
	public static void main(String... s) throws Exception {

		// INITIALIZATION
		if (s.length < 4) {
			System.err.println("Usage: SimpleApp <batch-duration> <bootstrap-servers> <input-topics> <output-topic>\n"
					+ " <batch-duration> is time in ms\n"
					+ " <bootstrap-servers> is a list of one or more Kafka bootstrap-servers\n"
					+ " <input-topics> is a list of one or more kafka topics to consume from\n"
					+ " <input-topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		Integer batchDuration = Integer.parseInt(s[0]);

		String brokers = s[1];
		Map<String, String> kafkaProperties = new HashMap<>();
		kafkaProperties.put("metadata.broker.list", brokers);
		// kafkaProperties.put("auto.offset.reset", "smallest");

		String inputTopics = s[2];
		Set<String> topicsSet = new HashSet<>(Arrays.asList(inputTopics.split(",")));

		String outputTopic = s[3];

		// ENCODERS
		Encoder<JSONInputDTO> testInputDTOEncoder = Encoders.bean(JSONInputDTO.class);

		// INITIALIZING SAPRK STREAM
		SparkConf conf = new SparkConf().setAppName("Simple Spark Streaming app").setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(batchDuration));

		// INITIALIZING SPARK SESSION
		SparkSession spark = SparkSession.builder().appName("SimpleApp").getOrCreate();

		// INITIALIZING KAFKA CONSUMER & READING FROM KAFKA
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaProperties, topicsSet);
		JavaDStream<String> msgStream = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception {
				return tuple2._2();
			}
		});

		// TRANSFORMING DSTREAM INTO RDD FOR PROCESSING
		JavaDStream<JSONOutputDTO> processedStream = msgStream
				.transform(new Function<JavaRDD<String>, JavaRDD<JSONOutputDTO>>() {
					@Override
					public JavaRDD<JSONOutputDTO> call(JavaRDD<String> javaRDD) throws Exception {
						JavaRDD<JSONInputDTO> jsonInputDTORDD = javaRDD.map(new Function<String, JSONInputDTO>() {
							@Override
							public JSONInputDTO call(String string) throws Exception {
								return new ObjectMapper().readValue(string, JSONInputDTO.class);
							}
						});
						
						// PROCESSING USING SPARK SQL
						Dataset<Row> rowDF = spark.createDataFrame(jsonInputDTORDD, JSONInputDTO.class);
						Dataset<JSONInputDTO> testInputDTODF = rowDF.as(testInputDTOEncoder);
						testInputDTODF.createOrReplaceTempView("testInput");
						Dataset<Row> processedDF = spark.sql(
								"select groupId, count(distinct UserID) as uniqueUsersPerGroup,count(id) as totalEventsPerGroup from testInput GROUP BY groupId");
						
						// CONVERTING PROCESSED DATAFRAME TO RDD AND THEN TO
						// REQUIRED OUTPUT JSON FORMAT
						JavaRDD<JSONOutputDTO> processedStream = processedDF.toJavaRDD()
								.map(new Function<Row, JSONOutputDTO>() {
							@Override
							public JSONOutputDTO call(Row row) throws Exception {
								JSONOutputDTO outputJson = new JSONOutputDTO();
								outputJson.setGroupId(row.getAs("groupId"));
								outputJson.setNumberOfUsersActive(row.getAs("uniqueUsersPerGroup"));
								outputJson.setTimestamp(Time.now());
								outputJson.setTotalNumberOfEvents(row.getAs("totalEventsPerGroup"));
								return outputJson;
							}
						});
						return processedStream;
					}
				});

		// SENDING PROCESSED OUTPUT TO KAFKA
		processedStream.foreachRDD(new VoidFunction<JavaRDD<JSONOutputDTO>>() {
			@Override
			public void call(JavaRDD<JSONOutputDTO> javaRDD) throws Exception {
				javaRDD.foreach(new VoidFunction<JSONOutputDTO>() {
					@Override
					public void call(JSONOutputDTO outputJson) throws Exception {
						TestProducer.pushToKafka(outputTopic, brokers,
								new ObjectMapper().writeValueAsString(outputJson));
					}
				});
			}
		});

		// STARTING SPARK STREAM
		jssc.start();
		jssc.awaitTermination();
	}
}
