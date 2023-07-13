package com.cursosdedesarrollo.ejercicios.fp;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.spark.sql.expressions.javalang.typed.avg;

public class EjerciciosPF {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);

    public static void main(String[] args) {
        String appName = "ResolucionPF";
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
        ventas.printSchema();
        ventas.show();

        // Transformar de Dataset<Row> a Dataset<Integer> (map)
        // Hacer que devuelva un Integer del Dataset<Integer> (reduce)

        Dataset<Row> unidadesRow = ventas.select("unidades_vendidas");
        unidadesRow.printSchema();
        unidadesRow.show();

        Dataset<Integer> unidades = ventas.map(
                (MapFunction<Row,Integer>) row ->
                row.getInt(1),
                Encoders.INT()
        );
        unidades.printSchema();
        unidades.show();

        Integer totales = unidades.reduce(
                (ReduceFunction<Integer>) Integer::sum
        );

        logger.info("Totales {}", totales);


        // Número máximo
        Integer maximo = unidades.reduce((ReduceFunction<Integer>) Math::max);

        // Número mínimo
        Integer minimo = unidades.reduce((ReduceFunction<Integer>) Math::min);

        // Cálculo rápido
        Long resultado = totales / unidades.count();

        logger.info("Número máximo: {}",maximo);
        logger.info("Número mínimo: {}", minimo);
        logger.info("Promedio: {}", resultado);

        Dataset<Row> rowResultado = unidades.selectExpr("avg(value)");
        logger.info("row: {}", rowResultado);
        rowResultado.printSchema();
        rowResultado.show();


        String salida = rowResultado.first().getAs("avg(value)").toString();
        logger.info("Media: {}",salida);

        // Crear una vista temporal del DataFrame
        unidades.createOrReplaceTempView("datos");

        // Cálculo de la media utilizando SQL
        Dataset<Row> res = spark.sql("SELECT AVG(value) AS media FROM datos");
        double media = res.first().getDouble(0);
        logger.info("Media con SQL: {}",media);

    }
}
