package com.cursosdedesarrollo;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Ejemplo00SesionRemota {
    public static void main(String[] args) {
        String appName = "Ejemplo00";
        String master ="spark://localhost:7077";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
    }

}
