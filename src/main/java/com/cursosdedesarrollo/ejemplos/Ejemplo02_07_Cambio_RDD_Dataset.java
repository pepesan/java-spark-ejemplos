package com.cursosdedesarrollo.ejemplos;


import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ejemplo02_07_Cambio_RDD_Dataset {

    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);

    public static void main(String[] args) {
        String appName = "Ejemplo_02_06_Cambio";
        String master = "local";
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // Crear un JavaRDD<String> de ejemplo
        JavaRDD<String> javaRDD = spark.sparkContext()
                .textFile("resources/people2.txt", 1)
                .toJavaRDD();

        // Definir el esquema de los datos
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                // Agrega más campos según tu caso
        });

        // Convertir el JavaRDD<String> a JavaRDD<Row>
        JavaRDD<Row> rowRDD = javaRDD.map(line -> {
            String[] fields = line.split(",");
            return RowFactory.create(fields[0], Integer.parseInt(fields[1]));
            // Crea más objetos RowFactory.create() según tu esquema
        });

        // Crear el Dataset<Row> utilizando el esquema
        Dataset<Row> dataset = spark.createDataFrame(rowRDD, schema);
        dataset.printSchema();
        // Mostrar el contenido del Dataset<Row>
        dataset.show();

        // Conseguir un JavaRDD<Row> desde un dataset<Row>
        JavaRDD<Row> javaRow = dataset.javaRDD();

        // Conseguir un JavaRDD<String> desde un JavaRDD<Row>
        JavaRDD<String> javaString =  javaRow.map(row -> {
            return row.getString(0)+","+row.getInt(1);
        });
        // recorrer el JavaRDD<String>
        javaString.foreach(element -> logger.info(element.toString()));

        // Cerrar la sesión de Spark
        spark.close();
        sc.close();
    }

}
