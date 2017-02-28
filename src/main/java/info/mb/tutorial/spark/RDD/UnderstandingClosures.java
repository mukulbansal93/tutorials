package info.mb.tutorial.spark.RDD;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnderstandingClosures {
	public static void main(String... s){
		SparkConf conf=new SparkConf().setAppName("Understanding Closures");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		List<Integer> integers=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> javaRDD=sc.parallelize(integers);
		int counter=0;
		//neither java 8 allows it, nor spark recommends this. Use Accumulator for this.
		//javaRDD.foreach(x-> counter += x);
		System.out.println("Counter value: " + counter);
		
		javaRDD.collect().forEach(x->System.out.println(x));
		javaRDD.take(5).forEach(x->System.out.println(x));
	}
}
