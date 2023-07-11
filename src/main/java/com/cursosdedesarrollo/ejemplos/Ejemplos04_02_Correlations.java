package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Ejemplos04_02_Correlations {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo04MLLib")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaDoubleRDD seriesX = sc.parallelizeDoubles(
                Arrays.asList(1.0, 2.0, 3.0, 3.0, 5.0));  // a series

        // must have the same number of partitions and cardinality as seriesX
        JavaDoubleRDD seriesY = sc.parallelizeDoubles(
                Arrays.asList(11.0, 22.0, 33.0, 33.0, 555.0));

        // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method.
        // If a method is not specified, Pearson's method will be used by default.
        double correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");
        logger.info("Correlación: {}", correlation);


        // note that each Vector is a row and not a column
        JavaRDD<Vector> data = sc.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(5.0, 33.0, 366.0)
                )
        );

        // calculate the correlation matrix using Pearson's method.
        // Use "spearman" for Spearman's method.
        // If a method is not specified, Pearson's method will be used by default.
        Matrix correlMatrix = Statistics.corr(data.rdd(), "pearson");
        logger.info("Matriz de correlación: {}", correlMatrix.toString());
    }
}
