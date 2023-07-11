package com.cursosdedesarrollo.ejemplos;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ejemplos03_04_MapReduce {
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

        // Map: Transforma cada elemento del RDD en un par (clave, valor)
        JavaPairRDD<Integer, Integer> mappedRDD = inputRDD.mapToPair(value -> new Tuple2<>(value % 2, value));

        // Reduce: Aplica una función de reducción para combinar los valores con la misma clave
        JavaPairRDD<Integer, Integer> reducedRDD = mappedRDD.reduceByKey(Integer::sum);

        // Recopila los resultados
        List<Tuple2<Integer, Integer>> result = reducedRDD.collect();

        // Imprime los resultados
        result.forEach(System.out::println);

        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}
