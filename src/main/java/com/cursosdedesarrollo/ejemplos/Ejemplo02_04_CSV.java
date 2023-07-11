package com.cursosdedesarrollo.ejemplos;


import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ejemplo02_04_CSV {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);

    public static void main(String[] args) {
        String appName = "Ejemplo_02_04_CSV";
        String master = "local";
        SparkSession spark = SparkSession.builder()
                .appName(appName)
                .master(master)
                .getOrCreate();
        // Cargar el archivo CSV en un DataFrame
        Dataset<Row> csvData = spark.read()
                .option("header", "true") // Si el archivo CSV tiene una fila de encabezado
                .csv("resources/iris.csv");

        // Mostrar el esquema del DataFrame
        csvData.printSchema();

        // Mostrar los primeros registros del DataFrame
        csvData.show();

        // Realizar operaciones adicionales en el DataFrame
        // ...

        // Cerrar la sesión de Spark
        spark.close();
    }

}
