package com.cursosdedesarrollo.ejemplos;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Ejemplos04_08_Kmeans {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("KMeansExample")
                .master("local")
                .getOrCreate();

        // Cargar los datos de flores Iris desde un archivo CSV
        Dataset<Row> irisData = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("resources/iris-multiclass.csv");

        // Seleccionar las columnas necesarias para el algoritmo K-means
        String[] featureColumns = {"SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm"};
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureColumns)
                .setOutputCol("features");

        Dataset<Row> inputData = assembler.transform(irisData).select("features");

        // Entrenar el modelo de K-means
        int k = 3; // Número de clusters
        int maxIter = 20; // Número máximo de iteraciones
        KMeans kmeans = new KMeans()
                .setK(k)
                .setMaxIter(maxIter);

        KMeansModel model = kmeans.fit(inputData);

        // Obtener los centroides de los clusters
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("features", DataTypes.createArrayType(DataTypes.DoubleType), false)
        });
        List<Row> rowList = Arrays.stream(model.clusterCenters())
                .map(vector -> RowFactory.create(vector.toArray()))
                .collect(Collectors.toList());
        Dataset<Row> centroids = spark.createDataFrame(rowList, schema);;

        System.out.println("Centroides:");
        centroids.show();

        // Predecir el cluster al que pertenecen los puntos de datos
        Dataset<Row> predictions = model.transform(inputData);

        System.out.println("Predicciones:");
        predictions.show();

        // Detener la sesión de Spark
        spark.stop();
    }
}
