package com.cursosdedesarrollo.ejemplos;

import com.cursosdedesarrollo.ejemplos.entities.Department;
import com.cursosdedesarrollo.ejemplos.entities.Employee;
import com.cursosdedesarrollo.ejemplos.entities.Employee2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class Ejemplos03_07_Union {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo05Procesado")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Employee2> employeeData = Arrays.asList(
                new Employee2("IT", "Alice"),
                new Employee2("HR", "Bob"),
                new Employee2("Finance", "Charlie")
        );
        List<Department> departmentData = Arrays.asList(
                new Department("IT", "New York"),
                new Department("HR", "London"),
                new Department("Sales", "Paris")
        );
        Dataset<Row> employeeDF = spark.createDataFrame(employeeData, Employee2.class);
        Dataset<Row> departmentDF = spark.createDataFrame(departmentData, Department.class);

        // Realiza la operación de unión
        Dataset<Row> unionDF = employeeDF.union(departmentDF);
        unionDF.printSchema();

        // Muestra los resultados
        unionDF.show();

        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}
