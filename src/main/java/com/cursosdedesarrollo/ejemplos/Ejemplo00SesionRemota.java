package com.cursosdedesarrollo.ejemplos;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Ejemplo00SesionRemota {
    public static void main(String[] args) throws InterruptedException {
        String appName = "Ejemplo00";
        String master ="spark://localhost:7077";

        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Thread.sleep(30000);
        sc.close();
    }

}
