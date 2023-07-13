package com.cursosdedesarrollo.ejemplos;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.expr;

public class Ejemplos03_10_Select {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo05Procesado")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


        // Crear el DataFrame de ejemplo
        List<Row> ventas = Arrays.asList(
                RowFactory.create("2023-07-01", "Producto A", 3, 10.5),
                RowFactory.create("2023-07-01", "Producto B", 2, 8.0),
                RowFactory.create("2023-07-02", "Producto A", 1, 10.5),
                RowFactory.create("2023-07-02", "Producto C", 4, 12.75),
                RowFactory.create("2023-07-03", "Producto B", 5, 8.0)
        );

        StructType schema = new StructType()
                .add("fecha", DataTypes.StringType, false)
                .add("producto", DataTypes.StringType, false)
                .add("cantidad", DataTypes.IntegerType, false)
                .add("precio_unitario", DataTypes.DoubleType, false);

        Dataset<Row> df = spark.createDataFrame(ventas, schema);
        df.printSchema();
        // Mostrar el resultado
        df.show();
        Dataset<Row> seleccionados = df.select("fecha", "producto");
        seleccionados.printSchema();
        // Muestra los datos resultantes
        seleccionados.show();
        seleccionados = df.select("cantidad", "precio_unitario");
        seleccionados.printSchema();
        // Muestra los datos resultantes
        seleccionados.show();



        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}

