package query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

public class Query1_Preprocessing {

    /**
     * Query 1 preprocessing, consisting in:
     * - filtering of initial dataset by deleting the first line and considering
     *   dates after 1 January;
     * - columns selection;
     * - join between the two considered datasets;
     * - sort dataset by date
     * @param datasetSum
     * @param dataset3
     * @return sorted dataset by date
     */
    public static JavaPairRDD<String, Tuple3<String, String, Integer>> preprocessing(JavaPairRDD<String, Integer> datasetSum, JavaRDD<String> dataset3) {

        String firstLine = dataset3.first();
        //select util columns from dataset 3: country, date, complete name of country, tot vaccine
        JavaPairRDD<String, Tuple2<Tuple3<String, String, String>, Integer>> joinDataset = dataset3
                .filter(row -> (!(row.equals(firstLine)) && row.split(",")[0].compareTo("2021-01-01")>=0))
                .mapToPair(line -> {
                    String[] lineSplit = line.split(",");
                    Tuple3<String, String, String> columns = new Tuple3<>(lineSplit[0],lineSplit[2],lineSplit[lineSplit.length-1]);
                    return new Tuple2<>((lineSplit[1]), columns);
                }).join(datasetSum);

        return joinDataset.mapToPair(row -> {
            Tuple3<String, String, Integer> columns = new Tuple3<>(row._2._1._2(), row._2._1._3(), row._2._2());
            return new Tuple2<>(String.valueOf(row._2._1._1()), columns);
        }).sortByKey(true).cache();
    }
}
