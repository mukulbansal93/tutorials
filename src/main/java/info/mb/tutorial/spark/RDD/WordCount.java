package info.mb.tutorial.spark.RDD;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 *  
 * @author MBansal
 */
public class WordCount {

	public static void main(String... s) {
		
		String logFile = "/usr/spark-2.1.0-bin-hadoop2.7/bin/spark-shell.cmd";
		SparkConf config = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(config);
		JavaRDD<String> javaRDD = sc.textFile(logFile).cache();

		JavaRDD<String> words = javaRDD.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String x) throws Exception {
				return Arrays.asList(x.split(" ")).iterator();
			}

		});

		JavaPairRDD<String, Integer> wordPairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		});

		JavaPairRDD<String, Integer> wordPairCount = wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer x, Integer y) throws Exception {
				return x + y;
			}
		});

		// CORRESSPONDING LAMBDAS
		JavaRDD<String> wordsL = javaRDD.flatMap((x) -> Arrays.asList(x.split(" ")).iterator());
		JavaPairRDD<String,Integer> wordCountL= wordsL.mapToPair(x -> new Tuple2(x,1));
		JavaPairRDD<String, Integer> wordPairCountL = wordCountL.reduceByKey((x,y)->x+y);
		
		//SINGLE LINE COMMAND FOR SCALA-SHELL
		//inputfile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_);

		wordPairCount.saveAsTextFile("/usr/spark-2.1.0-bin-hadoop2.7/bin/wordCount");
		
		System.out.println("===================================");
		words.collect().forEach(x -> System.out.println(x));
		System.out.println("===================================");
		wordPairs.collect().forEach(x -> System.out.println(x));
		System.out.println("===================================");
		wordPairCount.collect().forEach(x -> System.out.println(x));

		sc.stop();
	}

}
