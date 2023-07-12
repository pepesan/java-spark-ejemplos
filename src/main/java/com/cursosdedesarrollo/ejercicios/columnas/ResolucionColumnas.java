package com.cursosdedesarrollo.ejercicios.columnas;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;

public class ResolucionColumnas {

    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);

    public static void main(String[] args) {
        String appName = "ResolucionColumnas";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // Cargar el archivo CSV en un DataFrame
        Dataset<Row> csvData = spark.read()
                .option("header", "true") // Si el archivo CSV tiene una fila de encabezado
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .csv("resources/boston.csv");

        csvData.printSchema();
        csvData.show();
        String columnas = csvData.columns().toString();
        logger.info(columnas);
        Dataset<Row> colCrim = csvData.select("CRIM");
        colCrim.printSchema();
        colCrim.show();

        Column indus = functions.col("INDUS");
        csvData.orderBy(indus).printSchema();
        csvData.orderBy(indus).show();

        Dataset<Row> dfSuma = csvData.withColumn("suma", csvData.col("ZN").plus(csvData.col("INDUS")));
        dfSuma.printSchema();
        dfSuma.show();

        dfSuma = csvData.withColumn("suma", csvData.col("ZN").plus(2));
        dfSuma.printSchema();
        dfSuma.show();
        Dataset<Row> dfSinSuma = dfSuma.drop("suma");
        dfSinSuma.printSchema();
        dfSinSuma.show();
    }
}
