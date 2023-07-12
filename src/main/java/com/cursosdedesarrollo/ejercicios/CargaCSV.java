package com.cursosdedesarrollo.ejercicios;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CargaCSV {

    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        /*
            Ejercicio 01:
            Carga el fichero CSV desde la ruta resources/people.csv
         */
        String appName = "CargaCSV";
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
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv("resources/people.csv");

        // Mostrar el esquema del DataFrame
        csvData.printSchema();

        // Mostrar los primeros registros del DataFrame
        csvData.show();

        csvData.foreach(element -> {
            logger.info(element.toString());
        });
    }
}
