package com.cursosdedesarrollo.ejercicios.fp;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class EjerciciosPF {
    public static void main(String[] args) {
        String appName = "ResolucionColumnas";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> ventas = spark.createDataFrame(Arrays.asList(
                RowFactory.create("Producto A", 10),
                RowFactory.create("Producto B", 5),
                RowFactory.create("Producto C", 8),
                RowFactory.create("Producto A", 3),
                RowFactory.create("Producto C", 6)
        ), new StructType()
                .add("producto", DataTypes.StringType)
                .add("unidades_vendidas", DataTypes.IntegerType));
    }
}
