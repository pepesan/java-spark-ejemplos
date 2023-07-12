package com.cursosdedesarrollo.ejercicios;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CargaParquet {

    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        /*
            Ejercicio 03:
            Carga el fichero Parquet desde la ruta resources/iris.parquet
         */
        String appName = "CargaCSV";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> irisDataset = spark.read().parquet("resources/iris.parquet");
        // Mostrar el esquema del DataFrame
        irisDataset.printSchema();

        // Mostrar los primeros registros del DataFrame
        irisDataset.show();
    }
}
