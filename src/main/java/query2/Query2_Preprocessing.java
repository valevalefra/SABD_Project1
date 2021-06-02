package query2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;


public class Query2_Preprocessing {

    private static final String START_DATE = "2021-02-01";

    /**
     * Query 2 preprocessing, consisting in:
     * - filtering the dataset by deleting the first line and considering
     * - dates from the 1st of February;
     * - columns selection;
     * @param dataset2 containing administration date, age, number of female people vaccinated
     *                 and area name
     * @return preprocessed JavaPairRDD
     */
    public static JavaPairRDD<Tuple3<String, String, String>, Integer> preprocessing(JavaRDD<String> dataset2) {

        String firstLine = dataset2.first();

        return dataset2
                .filter(row-> row.split(",")[0].compareTo(START_DATE)>=0 && !(row.equals(firstLine)))
                .mapToPair( row -> {
                    String[] lineSplit = row.split(",");
                    //putting as key a tuple with date, age and area
                    return new Tuple2<>(new Tuple3<>(lineSplit[0], lineSplit[3],
                            lineSplit[lineSplit.length-1]), Integer.parseInt(lineSplit[5]));
        });
    }
}
