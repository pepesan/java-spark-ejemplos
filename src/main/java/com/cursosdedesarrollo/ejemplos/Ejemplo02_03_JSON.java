package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ejemplo02_03_JSON {

    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);

    public static void main(String[] args) {
        String appName = "Ejemplo_02_03_JSON";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> jsonData = spark.read().json("resources/people.json");

        // Mostrar el esquema del DataFrame
        jsonData.printSchema();

        // Mostrar los primeros registros del DataFrame
        jsonData.show();

        // Realizar operaciones adicionales en el DataFrame
        // ...

        Dataset<Row> jsonDataAray = spark.read().json("resources/peopleArray.json");

        // Mostrar el esquema del DataFrame
        jsonDataAray.printSchema();

        // Mostrar los primeros registros del DataFrame
        jsonDataAray.show();
        // Cerrar la sesión de Spark
        spark.close();
    }
}
