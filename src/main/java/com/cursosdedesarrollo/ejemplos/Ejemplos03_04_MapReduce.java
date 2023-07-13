package com.cursosdedesarrollo.ejemplos;

import com.cursosdedesarrollo.ejemplos.entities.Employee;
import com.cursosdedesarrollo.ejemplos.entities.Person;
import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Ejemplos03_04_MapReduce {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo05Procesado")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Crea un RDD a partir de una lista de enteros
        List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> inputRDD = sc.parallelize(inputList);
        System.out.println("inputRDD");
        inputRDD.foreach(element -> logger.info(element.toString()));
        // Map: Transforma cada elemento del RDD en un par (clave, valor)
        JavaPairRDD<Integer, Integer> mappedRDD = inputRDD.mapToPair(value -> new Tuple2<>(value % 2, value));
        System.out.println("mappedRDD");
        mappedRDD.foreach(element -> logger.info(element.toString()));

        // Reduce: Aplica una función de reducción para combinar los valores con la misma clave
        JavaPairRDD<Integer, Integer> reducedRDD = mappedRDD.reduceByKey(Integer::sum);
        System.out.println("reducedRDD");
        reducedRDD.foreach(element -> logger.info(element.toString()));

        // Recopila los resultados
        List<Tuple2<Integer, Integer>> result = reducedRDD.collect();

        // Imprime los resultados
        result.forEach(System.out::println);


        List<Employee> data = Arrays.asList(
                new Employee("IT", "Alice", 50000),
                new Employee("IT", "Bob", 60000),
                new Employee("HR", "Charlie", 55000),
                new Employee("HR", "David", 65000)
        );
        Dataset<Row> df = spark.createDataFrame(data, Employee.class);
        /*
        Dataset<Row> mappedDF = df.map(row -> RowFactory.create(
            row.getString(0), row.getString(1), row.getInt(2),
                Encoders.bean(Row.class)
        ));

         */

        Encoder<Long> longEncoder = Encoders.LONG();
        Dataset<Long> primitiveDS = spark.createDataset(Arrays.asList(1L, 2L, 3L), longEncoder);
        primitiveDS.printSchema();
        primitiveDS.show();
        Dataset<Long> transformedDS = primitiveDS.map(
                (MapFunction<Long, Long>) value -> value + 1L,
                longEncoder);
        transformedDS.printSchema();
        transformedDS.show();
        transformedDS.collect(); // Returns [2, 3, 4]

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> peopleDS = spark.read().json("resources/people.json").as(personEncoder);
        peopleDS.printSchema();
        peopleDS.show();
        Encoder<String> stringEncoder = Encoders.bean(String.class);


        Dataset<String> nombres = peopleDS.map(
                (MapFunction<Person, String>) person -> "Name: " + person.getName(),
                stringEncoder);
        nombres.printSchema();

        Dataset<Row> peopleDF = spark.read().json("resources/people.json");

        peopleDF.printSchema();
        peopleDF.show();
        Dataset<String> nombresDS = peopleDF.map(
                (MapFunction<Row, String>) person -> "Name: " + person.getString(0),
                stringEncoder);
        nombresDS.printSchema();



        Encoder<Row> rowEncoder = Encoders.bean(Row.class);
        Dataset<Row> modifiedPeople = peopleDF.map(
                (MapFunction<Row, Row>) row ->{
                    // cualquier código que tu quieras para modificar cada row
                    return RowFactory.create(row.getLong(0)+1, row.getString(1)+"!");
                },
                rowEncoder
        );
        modifiedPeople.printSchema();

        modifiedPeople.foreach(row -> {
            String salida = "Row: age: "+ row.getLong(0) + ", name: '"+ row.getString(1)+"'";
            logger.info(salida);
        });


        List<Integer> dataDF = Arrays.asList(1, 2, 3);
        Dataset<Integer> ds = spark.createDataset(dataDF, Encoders.INT());
        int reduced = ds.reduce(
                (ReduceFunction<Integer>) (act, acc) -> act + acc
        );
        logger.info("Reduced: {}",reduced);



        // Cierra la sesión de Spark al finalizar
        spark.stop();
    }
}
