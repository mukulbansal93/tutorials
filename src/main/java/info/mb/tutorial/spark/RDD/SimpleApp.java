package info.mb.tutorial.spark.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String... s) {
		String logFile = "/usr/spark-2.1.0-bin-hadoop2.7/bin/spark-shell.cmd";
		SparkConf config = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(config);
		JavaRDD<String> javaRDD = sc.textFile(logFile).cache();

		long numAs = javaRDD.filter(new Function<String, Boolean>() {

			public Boolean call(String s) throws Exception {
				return s.contains("a");
			}

		}).count();

		long numBs = javaRDD.filter(x -> x.contains("b")).count();

		System.out.println("Lines with a: " + numAs);
		System.out.println("Lines with b: " + numBs);

		sc.stop();
	}
}
