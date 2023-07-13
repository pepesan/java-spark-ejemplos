package com.cursosdedesarrollo.ejemplos;

import com.cursosdedesarrollo.ejemplos.entities.Person;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class Ejemplo03_02_Procesado {
    public static void main(String[] args) {
        String appName = "Ejemplo03_02_Procesado";
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
        // Muestra el contenido del DataFrame
        df.show();

        // Filtra los registros donde la edad sea mayor o igual a 30
        Dataset<Row> filteredDF = df.filter(functions.col("age").geq(30));
        filteredDF.show();

        // Agrupa por nombre y calcula el promedio de edad
        Dataset<Row> avgAgeDF = df.groupBy("name").agg(functions.avg("age").alias("avg_age"));
        avgAgeDF.show();

        // Ordena por edad de forma descendente
        Dataset<Row> sortedDF = df.orderBy(functions.col("age").desc());
        sortedDF.show();
        // Ordena por edad de forma ascendente
        Dataset<Row> sortedDFAsc = df.orderBy(functions.col("age").asc());
        sortedDFAsc.show();

        Dataset<Row> doubleSortedDF = df.orderBy(
                functions.col("age").asc(),
                functions.col("name").desc()
        );
        doubleSortedDF.show();

        // Calcula la suma de las edades
        long sumOfAges = df.agg(functions.sum("age")).head().getLong(0);
        System.out.println("Sum of ages: " + sumOfAges);

        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}
