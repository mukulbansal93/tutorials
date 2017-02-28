package info.mb.tutorial.spark.dataframe;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ProgrammaticallySpecifyingSchema {
	public static void main(String... s) {
		SparkSession spark = SparkSession.builder().appName("Programmatically Specifying the Schema").getOrCreate();
		String filePath = "/usr/spark-2.1.0-bin-hadoop2.7/examples/src/main/resources/people.txt";
		JavaRDD<String> stringRDD = spark.read().textFile(filePath).javaRDD();

		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}

		// CREATED A StructType instead of a DTO
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows
		/*
		 * JavaRDD<Row> personRDD=stringRDD.map(new Function<String, Row>() {
		 * 
		 * @Override public Row call(String record) throws Exception { String[]
		 * attributes=record.split(" "); return
		 * RowFactory.create(attributes[0],attributes[1].trim()); } });
		 */

		// CORRESSPONDING LAMBDA
		JavaRDD<Row> personRDD = stringRDD.map((record) -> {
			String[] attributes = record.split(",");
			return RowFactory.create(attributes[0], attributes[1].trim());
		});

		Dataset<Row> personDF = spark.createDataFrame(personRDD, schema);
		personDF.createOrReplaceTempView("person");
		spark.sql("select * from person").show();
	}
}
