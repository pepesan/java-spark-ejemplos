package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class Ejemplo02_02_LoadFile {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        String appName = "Ejemplo_02_03_LoadFile";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> distFile = sc.textFile("resources/data.txt");
        // Imprimir cada elemento del RDD
        distFile.foreach(element -> logger.info("texto: {}", element));
        distFile = distFile.cache();
        // Pillar la longitud de cada línea
        JavaRDD<Integer> lineLengths = distFile.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        logger.info("Longitud Total: {}", totalLength);
        // persistimos en memoria los datos
        lineLengths.persist(StorageLevel.MEMORY_ONLY());
        // Cerrar contexto
        sc.close();
        spark.close();

    }
}
