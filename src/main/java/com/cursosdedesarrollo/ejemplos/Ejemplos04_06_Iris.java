package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;

public class Ejemplos04_06_Iris {
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

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .load("resources/iris-multiclass.csv");

        df.printSchema();

        String[] inputColumns = {"SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm"};
        VectorAssembler assembler = new VectorAssembler().setInputCols(inputColumns).setOutputCol("features");
        Dataset<Row> featureSet = assembler.transform(df);

        // Dividir los datos en conjunto de entrenamiento (70%) y conjunto de prueba (30%)
        long seed = 5043;
        Dataset<Row>[] trainingAndTestSet = featureSet.randomSplit(new double[]{0.7, 0.3}, seed);
        Dataset<Row> trainingSet = trainingAndTestSet[0];
        Dataset<Row> testSet = trainingAndTestSet[1];

        // Entrenar el algoritmo basado en un Clasificador Random Forest con valores predeterminados
        RandomForestClassifier randomForestClassifier = new RandomForestClassifier().setSeed(seed);
        RandomForestClassificationModel model = randomForestClassifier.fit(trainingSet);

        // Probar el modelo con el conjunto de prueba
        Dataset<Row> predictions = model.transform(testSet);

        // Evaluar el modelo
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
        logger.info("accuracy: {}", evaluator.evaluate(predictions));

        spark.stop();
    }
}
