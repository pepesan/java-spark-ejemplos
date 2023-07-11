package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
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
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        // Paralelizar los datos
        JavaRDD<Integer> distData = sc.parallelize(data);
        // Imprimir cada elemento del RDD
        distData.foreach(element -> logger.info("RDD Elemento: {}", element));
    }
}
