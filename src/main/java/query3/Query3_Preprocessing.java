package query3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class Query3_Preprocessing {

    public static JavaPairRDD<String, Tuple2<String, Integer>> dataset3Preprocessing(JavaRDD<String> dataset3) {
        String firstLine = dataset3.first();
        JavaPairRDD<String, Tuple2<String, Integer>> filteredDataset = dataset3
        .filter(row->
            row.split(",")[0].compareTo("2020-12-27")>=0
                    && row.split(",")[0].compareTo("2021-05-31")<=0 &&
                    !(row.equals(firstLine)))
                .mapToPair( row -> {
                    String[] lineSplit = row.split(",");
                    return new Tuple2<>(lineSplit[lineSplit.length-1], new Tuple2<>(lineSplit[0],
                            Integer.parseInt(lineSplit[2])));
                    });

        return filteredDataset;
    }

    public static JavaPairRDD<String, Integer> totalPopulationPreprocessing(JavaRDD<String> totalPopulation) {
        String firstLine = totalPopulation.first();
        JavaPairRDD<String, Integer> totalDataset = totalPopulation
                .filter(row -> !(row.equals(firstLine)))
                .mapToPair(row -> {
                    String[] lineSplit = row.split(",");
                    return new Tuple2<>(lineSplit[0],
                            Integer.parseInt(lineSplit[lineSplit.length-1]));
                });
        return totalDataset;
    }
}
