package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.LoggerFactory;

public class Ejemplos04_07_Iris_Completo {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        // Reducir el número de LOG
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("Ejemplo08Irisv2")
                .setMaster("local");
        SparkContext sc = new SparkContext(conf);

        SparkSession spark = SparkSession.builder()
                .appName("CargaJSON")
                .config("log4j.rootCategory", "ERROR, console")
                .getOrCreate();

        Dataset<Row> irisData = spark.read().format("csv")
                .option("header", "true")
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .load("resources/iris-multiclass.csv");

        irisData.printSchema();

        // Definir el esquema de la estructura de los datos
//        StructType schema = DataTypes.createStructType(new StructField[]{
//                DataTypes.createStructField("SepalLengthCm", DataTypes.DoubleType, false),
//                DataTypes.createStructField("SepalWidthCm", DataTypes.DoubleType, false),
//                DataTypes.createStructField("PetalLengthCm", DataTypes.DoubleType, false),
//                DataTypes.createStructField("PetalWidthCm", DataTypes.DoubleType, false),
//                DataTypes.createStructField("label", DataTypes.StringType, false)
//        });

        // Asignar el esquema al DataFrame
        irisData = irisData.withColumn("SepalLengthCm", irisData.col("SepalLengthCm").cast(DataTypes.DoubleType))
                .withColumn("SepalWidthCm", irisData.col("SepalWidthCm").cast(DataTypes.DoubleType))
                .withColumn("PetalLengthCm", irisData.col("PetalLengthCm").cast(DataTypes.DoubleType))
                .withColumn("PetalWidthCm", irisData.col("PetalWidthCm").cast(DataTypes.DoubleType));

        // Crear un VectorAssembler para combinar las características en un solo vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm"})
                .setOutputCol("features");

        // Transformar los datos usando el VectorAssembler
        Dataset<Row> transformedData = assembler.transform(irisData);

        // Dividir los datos en conjuntos de entrenamiento y prueba
        double[] weights = {0.7, 0.3};
        long seed = 1234L;
        Dataset<Row>[] splits = transformedData.randomSplit(weights, seed);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Crear el modelo de clasificación
        LogisticRegression logisticRegression = new LogisticRegression()
                .setLabelCol("label")
                .setFeaturesCol("features")
                .setMaxIter(10);

        // Entrenar el modelo utilizando los datos de entrenamiento
        LogisticRegressionModel model = logisticRegression.fit(trainingData);

        // Realizar predicciones en los datos de prueba
        Dataset<Row> predictions = model.transform(testData);

        // Calcular la precisión (accuracy) del modelo
        long correctCount = predictions.select("label", "prediction")
                .where("label = prediction")
                .count();
        long totalCount = predictions.count();
        double accuracy = (double) correctCount / totalCount;

        // Imprimir la precisión del modelo
        logger.info("Accuracy: {}", accuracy);

        spark.stop();
    }
}
