package com.cursosdedesarrollo.ejercicios;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CargaJSON {

    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        /*
            Ejercicio 01:
            Carga el fichero CSV desde la ruta resources/people.csv
         */
        String appName = "CargaJSON";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        Dataset<Row> jsonData = spark.read()
                .option("multiLine", true)
                .json("resources/ejercicios/resultados.json");

        // Mostrar el esquema del DataFrame
        jsonData.printSchema();

        // Mostrar los primeros registros del DataFrame
        jsonData.show();

    }
}
