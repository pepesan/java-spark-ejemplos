package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Ejemplos04_05_RegresionLineal {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo04MLLib")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> training = spark.read().format("libsvm")
                .load("resources/sample_libsvm_data.txt");

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        // Ajustar el modelo
        LinearRegressionModel lrModel = lr.fit(training);

        // Imprimir los coeficientes e interceptación para la regresión lineal
        logger.info("Coefficients: {} Intercept: {}" , lrModel.coefficients(), lrModel.intercept());
        System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        // Resumir el modelo sobre el conjunto de entrenamiento y mostrar algunas métricas

        LinearRegressionSummary trainingSummary = lrModel.summary();
        trainingSummary.residuals().show();
        // logger.info("Residuals Count: {}" ,trainingSummary.residuals().collect().length);
        logger.info("RMSE: {}" ,trainingSummary.rootMeanSquaredError());
        logger.info("r2: {}" ,trainingSummary.r2());

    }
}
