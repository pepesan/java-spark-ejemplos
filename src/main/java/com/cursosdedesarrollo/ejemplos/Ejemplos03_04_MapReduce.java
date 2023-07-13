package com.cursosdedesarrollo.ejemplos;

import com.cursosdedesarrollo.ejemplos.entities.Employee;
import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ejemplos03_04_MapReduce {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo05Procesado")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Crea un RDD a partir de una lista de enteros
        List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> inputRDD = sc.parallelize(inputList);
        System.out.println("inputRDD");
        inputRDD.foreach(element -> logger.info(element.toString()));
        // Map: Transforma cada elemento del RDD en un par (clave, valor)
        JavaPairRDD<Integer, Integer> mappedRDD = inputRDD.mapToPair(value -> new Tuple2<>(value % 2, value));
        System.out.println("mappedRDD");
        mappedRDD.foreach(element -> logger.info(element.toString()));

        // Reduce: Aplica una función de reducción para combinar los valores con la misma clave
        JavaPairRDD<Integer, Integer> reducedRDD = mappedRDD.reduceByKey(Integer::sum);
        System.out.println("reducedRDD");
        reducedRDD.foreach(element -> logger.info(element.toString()));

        // Recopila los resultados
        List<Tuple2<Integer, Integer>> result = reducedRDD.collect();

        // Imprime los resultados
        result.forEach(System.out::println);


        List<Employee> data = Arrays.asList(
                new Employee("IT", "Alice", 50000),
                new Employee("IT", "Bob", 60000),
                new Employee("HR", "Charlie", 55000),
                new Employee("HR", "David", 65000)
        );
        Dataset<Row> df = spark.createDataFrame(data, Employee.class);
        /*
        Dataset<Row> mappedDF = df.map(row -> RowFactory.create(
            row.getString(0), row.getString(1), row.getInt(2),
                Encoders.bean(Row.class)
        ));

         */


        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}
