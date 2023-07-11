package com.cursosdedesarrollo.ejemplos;

import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Ejemplos04_03_LinearSVM {
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
        JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
        training.cache();
        JavaRDD<LabeledPoint> test = data.subtract(training);

        // Run training algorithm to build the model.
        int numIterations = 100;
        SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

        // Clear the default threshold.
        model.clearThreshold();

        // Compute raw scores on the test set.
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(p ->
                new Tuple2<>(model.predict(p.features()), p.label()));

        // Get evaluation metrics.
        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
        double auROC = metrics.areaUnderROC();

        System.out.println("Area under ROC = " + auROC);

        // Save and load model
        model.save(sc.sc(), "target/tmp/javaSVMWithSGDModel");
        SVMModel sameModel = SVMModel.load(sc.sc(), "target/tmp/javaSVMWithSGDModel");
    }
}
