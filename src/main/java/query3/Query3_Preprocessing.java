package query3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class Query3_Preprocessing {

    private static final String START_DATE = "2020-12-27";
    private static final String END_DATE = "2021-05-31";

    /**
     * Query 3 preprocessing, consisting in dataset 3 preprocessing:
     * - filtering of initial dataset by deleting the first line and considering
     *  dates between 27 December and 31 May;
     * - columns selection;
     * @param dataset3 containing area, date and number of vaccinations
     * @return selected columns
     */
    public static JavaPairRDD<String, Tuple2<String, Integer>> dataset3Preprocessing(JavaRDD<String> dataset3) {
        String firstLine = dataset3.first();

        return dataset3
        .filter(row->
            row.split(",")[0].compareTo(START_DATE)>=0
                    && row.split(",")[0].compareTo(END_DATE)<=0 &&
                    !(row.equals(firstLine)))
                .mapToPair( row -> {
                    String[] lineSplit = row.split(",");
                    //select area, date and number of vaccinations, putting as key area
                    return new Tuple2<>(lineSplit[lineSplit.length-1], new Tuple2<>(lineSplit[0],
                            Integer.parseInt(lineSplit[2])));
                    });
    }

    /**
     * Query 3 preprocessing, consisting in dataset of total population preprocessing:
     * - filtering of initial dataset by deleting the first line and considering
     * - columns selection;
     * @param totalPopulation dataset containing number of citizens, regions
     * @return selected columns
     */
    public static JavaPairRDD<String, Integer> totalPopulationPreprocessing(JavaRDD<String> totalPopulation) {
        String firstLine = totalPopulation.first();
        return totalPopulation
                .filter(row -> !(row.equals(firstLine)))
                .mapToPair(row -> {
                    String[] lineSplit = row.split(",");
                    //select area and number of population
                    return new Tuple2<>(lineSplit[0],
                            Integer.parseInt(lineSplit[lineSplit.length-1]));
                });
    }
}
