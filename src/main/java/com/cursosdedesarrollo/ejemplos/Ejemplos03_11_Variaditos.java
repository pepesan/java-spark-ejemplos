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

public class Ejemplos03_11_Variaditos {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo05Procesado")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


        // Crear un Dataset de ejemplo
        List<Row> rows = Arrays.asList(
                RowFactory.create("A"),
                RowFactory.create("B"),
                RowFactory.create("C"),
                RowFactory.create("A"),
                RowFactory.create("B")
        );
        StructType schema = new StructType().add("columna", DataTypes.StringType);
        Dataset<Row> dataset = spark.createDataFrame(rows, schema);

        // Eliminar duplicados
        Dataset<Row> distinctData = dataset.distinct();
        distinctData.printSchema();
        // Mostrar los resultados
        distinctData.show();

        // Crear un Dataset de ejemplo
        rows = Arrays.asList(
                RowFactory.create("John", "Doe", "30"),
                RowFactory.create("Jane", "Smith", "25")
        );
        schema = new StructType()
                .add("nombre", DataTypes.StringType)
                .add("apellido", DataTypes.StringType)
                .add("edad", DataTypes.StringType);
        dataset = spark.createDataFrame(rows, schema);

        // Eliminar la columna "edad"
        Dataset<Row> withoutColumn = dataset.drop("edad");
        withoutColumn.printSchema();
        // Mostrar los resultados
        withoutColumn.show();

        // Crear un Dataset de ejemplo con valores nulos
        rows = Arrays.asList(
                RowFactory.create("John", "Doe", null),
                RowFactory.create("Jane", "Smith", 25)
        );
        schema = new StructType()
                .add("nombre", DataTypes.StringType)
                .add("apellido", DataTypes.StringType)
                .add("cantidad", DataTypes.IntegerType);
        dataset = spark.createDataFrame(rows, schema);
        dataset.printSchema();
        // Mostrar los resultados
        dataset.show();
        // Eliminar filas con valores nulos
        Dataset<Row> withoutNulls = dataset.na().drop();
        withoutNulls.printSchema();
        // Mostrar los resultados
        withoutNulls.show();




        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}

