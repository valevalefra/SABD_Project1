package query1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import utility.CSV_Writer;
import utility.Date_Parser;
import utility.HDFS_Utils;
import utility.Tuple_Comparator2;

import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public class Query1_Main {

    public static void main (String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        //retrieving datasets from HDFS
        JavaRDD<String> dataset1 = sparkContext.textFile(HDFS_Utils.getDS1());
        JavaRDD<String> dataset3 = sparkContext.textFile(HDFS_Utils.getDS3());

        Instant start = Instant.now();

        Tuple_Comparator2<String, String> compare = new Tuple_Comparator2<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());

        JavaPairRDD<String, Tuple3<String, String, Integer>> sortDataset = Query1_Preprocessing.preprocessing(dataset1, dataset3);

        //JavaPairRDD operations to calculate the ratio between the total vaccinations and
        //the hubs number for each hub
        JavaPairRDD<Tuple2<String, String>, Double> resultDataset = sortDataset.
                mapToPair(
                        tuple -> {
                            Calendar calendar = Date_Parser
                                    .getCalendar(Date_Parser.getConvertedDate(tuple._1(), "yyyy-MM-dd"));
                            String month = Date_Parser.getDate(calendar, "yyyy-MM");
                            double num= Double.parseDouble(tuple._2._1());
                            double d= (double) (tuple._2._3());
                            double ratio = num/d;
                            return new Tuple2<>(new Tuple2<>(month, tuple._2._2()), ratio);
                        }
                );

        //ReduceByKey and sortByKey operations in order to obtain the average number of vaccinations
        //for each area and month
        JavaPairRDD<Tuple2<String, String>, Double> finalDataset = resultDataset.reduceByKey(Double::sum).sortByKey(compare, true);

        //Map operation containing the month formatting and the daily average number of vaccinations calculation,
        //by dividing for the number of days in each month
        JavaRDD<String> resultRDD= finalDataset.map(tuple-> {
                    Calendar calendar = Date_Parser
                            .getCalendar(Date_Parser.getConvertedDate(tuple._1._1(), "yyyy-MM"));
                    String month = Date_Parser.getDate(calendar,"MMMM");
                    double daysInMonth = Date_Parser.getDaysInMonth(calendar);
                    String finMonth = month.substring(0,1).toUpperCase(Locale.ROOT)
                            +month.substring(1).toLowerCase(Locale.ROOT);
                    int meanVaxPerMonthArea = (int) (tuple._2/daysInMonth);
                    return finMonth+","+tuple._1._2()+","+meanVaxPerMonthArea;
                });


        HDFS_Utils.writeRDDToHdfs(HDFS_Utils.getOutputPathQuery1(), resultRDD);

        //collect action to obtain the final list
        List<String> result = resultRDD.collect();
        Instant end = Instant.now();
        System.out.println("Query completed in " + Duration.between(start, end).toMillis() + " ms");
        CSV_Writer.query1CSV(result);
        sparkContext.close();
    }
}
