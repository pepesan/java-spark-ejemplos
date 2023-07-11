package com.cursosdedesarrollo;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Ejemplo01SesionLocal {
    public static void main(String[] args) {
        String appName = "Ejemplo";
        String master = "local";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
    }

}
