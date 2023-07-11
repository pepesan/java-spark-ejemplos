package com.cursosdedesarrollo.ejemplos;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Ejemplo01SesionLocal {
    public static void main(String[] args) {
        String appName = "Ejemplo01";
        String master = "local";

        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.close();
        spark.close();
    }

}
