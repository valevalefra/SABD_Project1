package query3;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;
import utility.Clustering_Utils;
import utility.HDFS_Utils;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Query3_Main {

    private static final String DATE_OF_PREDICTION = "2021-06-01";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 3");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        SparkSession spark = SparkSession
                .builder()
                .appName("Query3")
                .master("local")
                .getOrCreate();

        //retrieving datasets from HDFS
        JavaRDD<String> dataset3 = sc.textFile(HDFS_Utils.getDS3());
        JavaRDD<String> totalPopulation = sc.textFile(HDFS_Utils.getTotPopulation());

        JavaPairRDD<String, Integer> totalRDD = Query3_Preprocessing.totalPopulationPreprocessing(totalPopulation);
        JavaPairRDD<String, Tuple2<String, Integer>> filterRDD = Query3_Preprocessing.dataset3Preprocessing(dataset3);

        Instant start = Instant.now();
        //sum number of vaccinations for each area
        JavaPairRDD<String,Integer> tempRDD = filterRDD
                .mapToPair(row-> new Tuple2<>(row._1(), row._2._2()))
                .reduceByKey(Integer::sum);

        //create instance of Simple Regression and adding observations to the model: day and number of vaccinations
        JavaPairRDD<String, SimpleRegression> regRDD = filterRDD
                .mapToPair(row -> {
                    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
                    Date convertedCurrentDate = sdf1.parse(row._2._1());
                    long day = convertedCurrentDate.getTime();
                    SimpleRegression sr = new SimpleRegression();
                    sr.addData((double)day, (double)row._2._2());
                    return new Tuple2<>(row._1(), sr);
                });

        //for each area return calculated values of simple regression
        JavaPairRDD<String, SimpleRegression> reducedRDD = regRDD
                .reduceByKey((a,b) -> {
                    a.append(b);
                    return a;
                });

        /*
          for each area predict number of vaccinations at 1 June. Join RDD
          in order to obtain columns: area and the estimate of the percentage
          of the population vaccinated on 1 June
         */
        JavaRDD<Tuple2<String, Double>> resultRDD =  reducedRDD
                .mapToPair(row -> {
                    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
                    Date monthConv = sdf1.parse(DATE_OF_PREDICTION);
                    double firstJune = monthConv.getTime();
                    double newVax = row._2().predict(firstJune);
                    return new Tuple2<>(row._1(), (int)newVax);
                }).join(totalRDD).join(tempRDD)
                .map(row->  new Tuple2<>(row._1(), (double)(row._2._1._1()+row._2._2())/row._2._1._2()));

        //collect action to obtain the final list
        List<Tuple2<String, Double>> data = resultRDD.collect();
        Encoder<Tuple2<String, Double>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE());
        Dataset<Row> train = spark.createDataset(data, encoder).toDF("area", "total%");
        List<List<String>> list = Clustering_Utils.clustering(train);
        JavaRDD<String> rddKMeans = sc.parallelize(list.get(0));
        JavaRDD<String> rddBisecting = sc.parallelize(list.get(1));
        HDFS_Utils.writeRDDToHdfs(HDFS_Utils.getOutputPathQuery3KMeans(), rddKMeans);
        HDFS_Utils.writeRDDToHdfs(HDFS_Utils.getOutputPathQuery3Bisecting(), rddBisecting);
        Instant end = Instant.now();
        System.out.println("Query 3 completed in " + Duration.between(start, end).toMillis() + " ms");

    }
}
