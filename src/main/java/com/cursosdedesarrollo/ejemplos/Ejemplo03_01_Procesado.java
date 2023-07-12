package com.cursosdedesarrollo.ejemplos;


import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Ejemplo03_01_Procesado {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        String appName = "Ejemplo03_01_Procesado";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // Lee el fichero CSV
        Dataset<Row> csv = spark.read()
                .option("header", "true") // Si el archivo CSV tiene una fila de encabezado
                .option("inferSchema", "true")
                .option("delimiter", ",")
                .csv("resources/sample.csv").cache();
        csv.printSchema();
        csv.show();

        // Filtra el usuario "me" y persiste
        Dataset<Row> result = csv.filter(
                (FilterFunction<Row>)row ->
                        !row.getAs("user").equals("me")
        );
        result.persist(StorageLevel.DISK_ONLY());

        // Imprime el resultado
        result.foreach(
                (ForeachFunction<Row>) element ->
                        logger.info("Row {}", element)
        );
        // Cierra la sesión de Spark
        spark.close();
        sc.close();
    }

}
