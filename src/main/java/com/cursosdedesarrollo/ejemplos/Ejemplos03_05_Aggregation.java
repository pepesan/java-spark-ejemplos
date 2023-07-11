package com.cursosdedesarrollo.ejemplos;

import com.cursosdedesarrollo.ejemplos.entities.Employee;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class Ejemplos03_05_Aggregation {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo05Procesado")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Employee> data = Arrays.asList(
                new Employee("IT", "Alice", 50000),
                new Employee("IT", "Bob", 60000),
                new Employee("HR", "Charlie", 55000),
                new Employee("HR", "David", 65000)
        );
        Dataset<Row> df = spark.createDataFrame(data, Employee.class);

        // Realiza la operación de groupBy y las agregaciones
        Dataset<Row> result = df.groupBy("department")
                .agg(
                        functions.sum("salary").alias("total_salary"),
                        functions.avg("salary").alias("avg_salary"),
                        functions.min("salary").alias("min_salary"),
                        functions.max("salary").alias("max_salary")
                );

        // Muestra los resultados
        result.show();

        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}

