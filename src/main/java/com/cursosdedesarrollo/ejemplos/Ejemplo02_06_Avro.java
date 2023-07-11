package com.cursosdedesarrollo.ejemplos;


import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ejemplo02_06_Avro {

    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);

    public static void main(String[] args) {
        String appName = "Ejemplo_02_05_CSV";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // Cargar los datos desde el archivo Avro
        Dataset<Row> userData = spark.read()
                .format("avro")
                .load("resources/users.avro");
        userData.printSchema();
        // Mostrar los datos cargados
        userData.show();

        // Realizar operaciones en los datos
        // Por ejemplo, obtener el recuento de usuarios
        long userCount = userData.count();
        System.out.println("Recuento de usuarios: " + userCount);

        // Filtrar los usuarios con un alter ego específico
        Dataset<Row> filteredData = userData.filter(userData.col("favorite_color").equalTo("red"));
        filteredData.show();

        // Cerrar la sesión de Spark
        spark.close();
    }

}
