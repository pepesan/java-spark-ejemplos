package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class Ejemplo02_01_Paralelize {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {

        String appName = "Ejemplo_02_03_Paralelize";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        // Paralelizar los datos
        JavaRDD<Integer> distData = sc.parallelize(data).cache().persist(StorageLevel.DISK_ONLY());
        // manipulación de datos
        JavaRDD<Integer> datosManipulados = distData.map(element -> element+1);
        // Imprimir cada elemento del RDD
        datosManipulados.foreach(
                element ->
                //{
                        logger.info("RDD Elemento: {}", element)
                                //;
                //}
        );
        List<Integer> dataFinales = datosManipulados.collect();

    }
}
