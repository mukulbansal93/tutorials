package info.mb.tutorial.spark.dataframe;

import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import info.mb.tutorial.spark.dto.Person;

public class EncoDecoDataset {

	public static void main(String... s) {

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL Encoding Decoding example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Person person = new Person();
		person.setName("Mukul");
		person.setAge(23);

		//CREATING A DATASET FROM A DTO 
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> personDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		personDS.show();

		// ENCODERS FOR PRIMITIVE DTASET ARE PROVIDED BY SPARK
		Encoder<Integer> intEncoder = Encoders.INT();
		Dataset<Integer> intDS = spark.createDataset(Arrays.asList(1, 2, 3, 4, 5), intEncoder);
		Dataset<Integer> transformedDS = intDS.map((x) -> x + 1, intEncoder);
		transformedDS.collect();
		transformedDS.show();
		
		//CREATING A DATASET FROM FILE CONTENTS
		String path="/usr/spark-2.1.0-bin-hadoop2.7/examples/src/main/resources/people.json";
		Dataset<Person> personDSFromFile = spark.read().json(path).as(personEncoder);
		personDSFromFile.show();
	}
}
