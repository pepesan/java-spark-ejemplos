package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Ejemplos04_09_Regresion_BostonPrices {

    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder()
                .appName("KMeansExample")
                .master("local")
                .getOrCreate();

        // Cargar el archivo CSV como un DataFrame
        Dataset<Row> dataset = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("resources/boston.csv");
        /// Convertir características en un vector usando VectorAssembler
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"CRIM", "ZN", "INDUS","CHAS", "NOX","RM", "AGE","DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT" })
                .setOutputCol("features");
        Dataset<Row> assembledData = assembler.transform(dataset).select("MEDV", "features");

        // Dividir el dataset en conjuntos de entrenamiento y prueba
        Dataset<Row>[] splits = assembledData.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        dataset.printSchema();
        // Definir el modelo de regresión lineal
        LinearRegression lr = new LinearRegression()
                .setLabelCol("MEDV")
                .setFeaturesCol("features");

        // Entrenar el modelo utilizando el conjunto de entrenamiento
        LinearRegressionModel model = lr.fit(trainingData);

        // Realizar predicciones en el conjunto de prueba
        Dataset<Row> predictions = model.transform(testData);

        // Evaluar el modelo utilizando una métrica de evaluación
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("MEDV")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) = " + rmse);
        LocalDateTime currentDate = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        String formattedDate = currentDate.format(formatter);
        String outPutPath= "salida/linear_regression_model"+formattedDate;
        // Guardar el modelo entrenado en un archivo
        model.save(outPutPath);

        // Cargar el modelo desde el archivo
        LinearRegressionModel loadedModel = LinearRegressionModel.load(outPutPath);



        // Detener la sesión de Spark
        spark.stop();
    }

    public static String[] getColumnNames(Dataset<Row> dataset) {
        return dataset.columns();
    }
}
