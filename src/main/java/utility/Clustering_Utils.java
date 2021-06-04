package utility;

import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import query3.Query3_Result;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


public class Clustering_Utils {
    private static int CLUSTERS = 5;

    /**
     * Function to realize clustering algorithms
     * @param train dataset to train
     * @return a list containing the adjusted list of kmeans and
     * bisecting kmenas results
     */
    public static List<List<String>> clustering(Dataset<Row> train) {
        List<List<String>> listData = new ArrayList<>();
        VectorAssembler va = new VectorAssembler()
                .setInputCols(new String[] {"total%"})
                .setOutputCol("features");
        List<Query3_Result> kMeansResults = new ArrayList<>();
        List<Query3_Result> bisectingKMeansResults = new ArrayList<>();
        Dataset<Row> transData = va.transform(train);
        //iterating through the numbers of clusters in order to obtain
        //performances with k from 2 to 5
        for (int k=2; k<=CLUSTERS; k++) {
            Query3_Result kMeansSilhouette = kMeans(k, transData);
            kMeansResults.add(kMeansSilhouette);
            Query3_Result bisectingKMeansSilhouette = bisectingKMeans(k, transData);
            bisectingKMeansResults.add(bisectingKMeansSilhouette);
        }
        listData.add(CSV_Writer.clusteringCSV(kMeansResults, 0));
        listData.add(CSV_Writer.clusteringCSV(bisectingKMeansResults, 1));
        return listData;
    }

    /**
     * Function to realize bisecting kmeans clustering algorithm
     * @param k number of clusters
     * @param transData transformed dataset with features selected
     * @return an object of type Query3_Result
     */
    private static Query3_Result bisectingKMeans(int k, Dataset<Row> transData) {
        Instant startBisectingKMeans = Instant.now();
        BisectingKMeans bkm = new BisectingKMeans().setK(k).setSeed(1);
        BisectingKMeansModel model = bkm.fit(transData);
        // Make predictions
        Query3_Result result = new Query3_Result();
        Dataset<Row> predictions = model.transform(transData);
        Instant endBisectingKMeans = Instant.now();
        long timeBisectingKMeans = Duration.between(startBisectingKMeans, endBisectingKMeans).toMillis();
        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double eval = evaluator.evaluate(predictions);
        result.setDataset(predictions);
        result.setEval(eval);
        result.setTime(timeBisectingKMeans);
        return result;
    }

    /**
     * Function to realize kmeans clustering algorithm
     * @param k number of clusters
     * @param transData transformed dataset with features selected
     * @return an object of type Query3_Result
     */
    private static Query3_Result kMeans(int k, Dataset<Row> transData) {
        Instant startKMeans = Instant.now();
        KMeans kmeans = new KMeans().setK(k).setSeed(1L);
        KMeansModel model =  kmeans.fit(transData);
        // Make predictions
        Query3_Result result = new Query3_Result();
        Dataset<Row> predictions = model.transform(transData);
        Instant endKMeans = Instant.now();
        long timeKMeans = Duration.between(startKMeans, endKMeans).toMillis();
        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double eval = evaluator.evaluate(predictions);
        result.setDataset(predictions);
        result.setEval(eval);
        result.setTime(timeKMeans);
        return result;
    }
}