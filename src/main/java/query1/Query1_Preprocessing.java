package query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

public class Query1_Preprocessing {

    private static final String START_DATE = "2021-01-01";
    private static final String END_DATE = "2021-05-31";

    /**
     * Query 1 preprocessing, consisting in:
     * - filtering of initial dataset by deleting the first line and considering
     *   dates after 1st January to the 31st of May;
     * - columns selection;
     * - join between the two considered datasets;
     * - sort dataset by date
     * @param dataset1 containing regions and hubs names
     * @param dataset3 containing data, regions (id and name) and total vaccinations
     * @return sorted dataset by date
     */
    public static JavaPairRDD<String, Tuple3<String, String, Integer>> preprocessing(JavaRDD<String> dataset1, JavaRDD<String> dataset3) {

        String firstLine1 = dataset1.first();
        String firstLine2 = dataset3.first();

        JavaRDD<String> useful_csv_rows = dataset1.filter(row -> !(row.equals(firstLine1)));

        JavaPairRDD<String, Integer> rddSum = useful_csv_rows.mapToPair(line -> {
            String[] lineSplit = line.split(",");
            return new Tuple2<>((lineSplit[0]), 1);}).reduceByKey(Integer::sum);

        //joinDataset returns: country, date, complete name of country, total vaccinations
        JavaPairRDD<String, Tuple2<Tuple3<String, String, String>, Integer>> joinRdd = dataset3
                .filter(row -> (!(row.equals(firstLine2))
                        && row.split(",")[0].compareTo(START_DATE)>=0)
                        && row.split(",")[0].compareTo(END_DATE)<=0)
                .mapToPair(line -> {
                    String[] lineSplit = line.split(",");
                    //selecting date, number of vaccinations and region complete name from the first dataset
                    Tuple3<String, String, String> columns = new Tuple3<>(lineSplit[0],lineSplit[2],lineSplit[lineSplit.length-1]);
                    //putting as key the region id
                    return new Tuple2<>((lineSplit[1]), columns);
                }).join(rddSum);

        return joinRdd.mapToPair(row -> {
            Tuple3<String, String, Integer> columns = new Tuple3<>(row._2._1._2(), row._2._1._3(), row._2._2());
            return new Tuple2<>(String.valueOf(row._2._1._1()), columns);
        }).sortByKey(true).cache();
    }
}
