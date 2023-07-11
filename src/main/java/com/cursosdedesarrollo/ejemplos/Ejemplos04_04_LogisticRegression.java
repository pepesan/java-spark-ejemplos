package com.cursosdedesarrollo.ejemplos;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Ejemplos04_04_LogisticRegression {
    private static final Logger logger = LoggerFactory.getLogger(SLF4JLog.class);
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Ejemplo04MLLib")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        String path = "resources/sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), path).toJavaRDD();

        // Split initial RDD into two... [60% training data, 40% testing data].
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
        JavaRDD<LabeledPoint> training = splits[0].cache();
        JavaRDD<LabeledPoint> test = splits[1];

        // Run training algorithm to build the model.
        LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(training.rdd());

        // Compute raw scores on the test set.
        JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(p ->
                new Tuple2<>(model.predict(p.features()), p.label()));

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double accuracy = metrics.accuracy();
        System.out.println("Accuracy = " + accuracy);

        // Save and load model
        model.save(sc.sc(), "target/tmp/javaLogisticRegressionWithLBFGSModel");
        LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc.sc(),
                "target/tmp/javaLogisticRegressionWithLBFGSModel");
    }
}
