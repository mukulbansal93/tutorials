package info.mb.tutorial.spark.dataframe;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {
	public static void main(String... s) {

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();
		String path = "/usr/spark-2.1.0-bin-hadoop2.7/examples/src/main/resources/people.json";
		Dataset<Row> df = spark.read().json(path);
		df.printSchema();
		df.select("name").show();
		/*
		 * NOT SO EFFICIENT APPROACH
		 * 
		 * df.col("name"); df.col("age").plus(1);
		 */
		df.show();
		df.select(col("name"), col("age").plus(1)).show();
		df.filter(col("age").gt(20)).show();
		df.groupBy("age").count().show();

		// RUNNING SQL QUERIES
		df.createOrReplaceTempView("people");
		Dataset<Row> sqlDf = spark.sql("Select * from people");
		sqlDf.show();

		// CREATING GLOBAL TEMP VIEW
		try {
			df.createGlobalTempView("people");
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Global temporary view is tied to a system preserved database
		// `global_temp`
		spark.sql("SELECT * FROM global_temp.people").show();

		// GLOBAL TEMPORART VIEW IS CROSS-SESSION
		spark.newSession().sql("SELECT * FROM global_temp.people").show();

	}
}
