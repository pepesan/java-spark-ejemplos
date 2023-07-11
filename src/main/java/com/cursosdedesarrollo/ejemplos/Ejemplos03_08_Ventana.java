package com.cursosdedesarrollo.ejemplos;

import com.cursosdedesarrollo.ejemplos.entities.Sale;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class Ejemplos03_08_Ventana {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo05Procesado")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Sale> data = Arrays.asList(
                new Sale("2022-01-01", "A", 100),
                new Sale("2022-01-02", "A", 200),
                new Sale("2022-01-03", "A", 150),
                new Sale("2022-01-01", "B", 300),
                new Sale("2022-01-02", "B", 250),
                new Sale("2022-01-03", "B", 180)
        );
        Dataset<Row> df = spark.createDataFrame(data, Sale.class);

        // Define una ventana por producto y orden por fecha
        WindowSpec windowSpec = Window.partitionBy("product").orderBy("date");

        // Utiliza funciones de ventana para realizar cálculos en el conjunto de filas
        Dataset<Row> result = df.withColumn("row_number", functions.row_number().over(windowSpec))
                .withColumn("rank", functions.rank().over(windowSpec))
                .withColumn("revenue_lag", functions.lag("revenue", 1).over(windowSpec))
                .withColumn("revenue_lead", functions.lead("revenue", 1).over(windowSpec));

        // Muestra los resultados
        result.show();

        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}

