package info.mb.tutorial.spark.RDD;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

/**
 *  
 * @author MBansal
 */
public class PiEstimation {
	public static void main(String... s) {

		int NUM_OF_SAMPLES = Integer.parseInt(s[0]);

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Pi-Estimation");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> list = new ArrayList<>();
		for (int i = 0; i <= NUM_OF_SAMPLES; i++)
			list.add(i);
		
		Long count = sc.parallelize(list).filter((returnVal) -> {
			Double x = Math.random();
			Double y = Math.random();
			return x * x + y * y < 1;
		}).count();

		System.out.println("Pi is roughly " + 4.0 * count / NUM_OF_SAMPLES);
	}
}
