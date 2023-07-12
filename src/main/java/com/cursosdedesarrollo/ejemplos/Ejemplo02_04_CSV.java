package com.cursosdedesarrollo.ejemplos;


import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // Cargar el archivo CSV en un DataFrame
        Dataset<Row> csvData = spark.read()
                .option("header", "false") // Si el archivo CSV tiene una fila de encabezado
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .csv("resources/iris.csv");

        // Mostrar el esquema del DataFrame
        csvData.printSchema();

        // Mostrar los primeros registros del DataFrame
        csvData.show();

        // Realizar operaciones adicionales en el DataFrame
        // ...

        Dataset<Row> csvDataHeader = spark.read()
                .option("header", "true") // Si el archivo CSV tiene una fila de encabezado
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .csv("resources/iris_header.csv");
        // Mostrar el esquema del DataFrame
        csvDataHeader.printSchema();

        // Mostrar los primeros registros del DataFrame
        csvDataHeader.show();
        // Cerrar la sesión de Spark

        // Convertir el Dataset<Row> a JavaRDD<Row>
        JavaRDD<Row> javaRDD = csvDataHeader.javaRDD();

        // Imprimir el contenido del JavaRDD<Row>
        javaRDD.foreach(row -> logger.info(row.toString()));

        // Convertir el JavaRDD<Row> a Dataset<Row>
        Dataset<Row> convertedDataset = spark.createDataFrame(javaRDD, csvDataHeader.schema());

        // Mostrar el contenido del Dataset<Row> convertido
        convertedDataset.show();

        spark.close();
    }

}
