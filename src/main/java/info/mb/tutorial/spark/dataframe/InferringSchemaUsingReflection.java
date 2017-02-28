package info.mb.tutorial.spark.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import info.mb.tutorial.spark.dto.Person;

public class InferringSchemaUsingReflection {
	public static void main(String... s) {
		SparkSession spark = SparkSession.builder().appName("InferringSchemaUsingReflection").getOrCreate();
		String filePath = "/usr/spark-2.1.0-bin-hadoop2.7/examples/src/main/resources/people.txt";
		JavaRDD<Person> personRDD = spark.read().textFile(filePath).toJavaRDD().map(new Function<String, Person>() {

			@Override
			public Person call(String line) throws Exception {
				String[] parts = line.split(",");
				Person person = new Person();
				person.setName(parts[0]);
				person.setAge(Integer.parseInt(parts[1].trim()));
				return person;
			}
		});

		Dataset<Row> personDS = spark.createDataFrame(personRDD, Person.class);
		personDS.createOrReplaceTempView("person");

		Dataset<Row> teenagersDF = spark.sql("select * from person where age between 13 and 19");
		teenagersDF.show();

		// The columns of a row in the result can be accessed by field index
		Dataset<String> teenagersByIndexDF = personDS.map(new MapFunction<Row, String>() {

			@Override
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(1);
			}
		}, Encoders.STRING());
		teenagersByIndexDF.show();

		// or by field name
		Dataset<Person> teenagersByFieldNameDF = personDS.map(new MapFunction<Row, Person>() {

			@Override
			public Person call(Row row) throws Exception {
				Person person = new Person();
				person.setName("name-"+row.getAs("name"));
				person.setAge(row.getAs("age"));
				return person;
			}
		}, Encoders.bean(Person.class));
		teenagersByFieldNameDF.show();
	}
}
