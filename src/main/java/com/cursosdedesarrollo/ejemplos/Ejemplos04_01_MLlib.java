package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Ejemplos04_01_MLlib {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo04MLLib")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Vector> mat = sc.parallelize(
                Arrays.asList(
                        Vectors.dense(1.0, 10.0, 100.0),
                        Vectors.dense(2.0, 20.0, 200.0),
                        Vectors.dense(3.0, 30.0, 300.0)
                )
        ); // an RDD of Vectors

        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
        logger.info("Media {}", summary.mean());  // a dense vector containing the mean value for each column
        logger.info("Varianza {}", summary.variance()); // column-wise variance
        logger.info("Numero no cero {}", summary.numNonzeros()); // number of nonzeros in each column
    }
}
