package com.cursosdedesarrollo.ejemplos;

import com.cursosdedesarrollo.ejemplos.entities.Person;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class Ejemplos03_03_Procesado {
    public static void main(String[] args) {
        String appName = "Ejemplo03_03_Procesado";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Person> data = Arrays.asList(
                new Person("Alice", 25L),
                new Person("Bob", 30L),
                new Person("Charlie", 35L),
                new Person("Alice", 40L)
        );
        Dataset<Row> df = spark.createDataFrame(data, Person.class);
        df.printSchema();
        df.show();

        // Utilizando funcionalidades de columnas para Añadir Columna
        Dataset<Row> modifiedDF =
                df.withColumn("age_plus_two", df.col("age").plus(2));
        modifiedDF.printSchema();
        modifiedDF.show();

        // Utilizando funcionalidades de columnas para modificar una columna

        modifiedDF =
                df.withColumn("age", df.col("age").plus(2));
        modifiedDF.printSchema();
        modifiedDF.show();

        // Utilizando funcionalidades de columnas  para añadir varias columnas
        modifiedDF =
                df
                    .withColumn("age_plus_two", addTwoToAge(df.col("age")))
                    .withColumn("name_uppercase", uppercaseName(df.col("name")))
                    .withColumn("age_category", categorizeAge(df.col("age")));
        modifiedDF.printSchema();
        modifiedDF.show();

        // borrar una columna
        modifiedDF = modifiedDF.drop("age_category");
        modifiedDF.printSchema();
        modifiedDF.show();
        // borrar varias columnas
        modifiedDF = modifiedDF.drop("age_plus_two", "name_uppercase");
        modifiedDF.printSchema();
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
