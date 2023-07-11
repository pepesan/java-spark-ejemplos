package com.cursosdedesarrollo.ejemplos;

import com.cursosdedesarrollo.ejemplos.entities.Person;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class Ejemplos03_03_Procesado {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo05Procesado")
                .master("local")
                .getOrCreate();

        List<Person> data = Arrays.asList(
                new Person("Alice", 25),
                new Person("Bob", 30),
                new Person("Charlie", 35),
                new Person("Alice", 40)
        );
        Dataset<Row> df = spark.createDataFrame(data, Person.class);

        // Utilizando funcionalidades de columnas
        Dataset<Row> modifiedDF = df.withColumn("age_plus_two", addTwoToAge(df.col("age")))
                .withColumn("name_uppercase", uppercaseName(df.col("name")))
                .withColumn("age_category", categorizeAge(df.col("age")));

        modifiedDF.show();

        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }

    public static Column addTwoToAge(Column column) {
        return column.plus(2);
    }

    public static Column uppercaseName(Column column) {
        return functions.upper(column);
    }

    public static Column categorizeAge(Column column) {
        return functions.when(column.lt(30), "Young")
                .when(column.lt(40), "Middle-aged")
                .otherwise("Elderly");
    }
}
