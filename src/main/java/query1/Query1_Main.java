package query1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import utility.CSV_Writer;
import utility.HDFS_Utils;
import utility.Tuple_Comparator2;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.util.*;

public class Query1_Main {
    public static void main (String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> dataset1 = sparkContext.textFile(HDFS_Utils.getDS1());
        JavaRDD<String> dataset3 = sparkContext.textFile(HDFS_Utils.getDS3());

        Instant start = Instant.now();

        Tuple_Comparator2 compare = new Tuple_Comparator2<>(Comparator.<Date>naturalOrder(), Comparator.<String>naturalOrder());

        String firstLine = dataset1.first();
        JavaRDD<String> useful_csv_rows = dataset1.filter(row -> !(row.equals(firstLine)));

        JavaPairRDD<String, Integer> datasetSum = useful_csv_rows.mapToPair(line -> {
            String[] lineSplit = line.split(",");
            return new Tuple2<>((lineSplit[0]), 1);}).reduceByKey(Integer::sum);

        JavaPairRDD<String, Tuple3<String, String, Integer>> sortDataset = Query1_Preprocessing.preprocessing(datasetSum, dataset3);
        //return ((month, country)), vaccianti/totale centri)
        JavaPairRDD<Tuple2<String, String>, Double> resultDataset = sortDataset.
                mapToPair(
                        tuple -> {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            Date convertedCurrentDate = sdf.parse(tuple._1());
                            Calendar calendar = new GregorianCalendar(Locale.ITALIAN);
                            calendar.setTime(convertedCurrentDate);
                            SimpleDateFormat monthDate = new SimpleDateFormat("yyyy-MM");
                            String month = monthDate.format(calendar.getTime());
                            double num= Double.parseDouble(tuple._2._1());
                            double d= (double) (tuple._2._3());
                            double ratio = num/d;
                            return new Tuple2<>(new Tuple2<>(month, tuple._2._2()), ratio);
                        }
                );

        JavaPairRDD<Tuple2<String, String>, Double> finalDataset = resultDataset.reduceByKey(Double::sum).sortByKey(compare);
        JavaRDD<String> resultRDD= finalDataset.map(tuple-> {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
                    Date convertedCurrentDate = sdf.parse(String.valueOf(tuple._1._1()));
                    Calendar calendar = new GregorianCalendar(Locale.ITALIAN);
                    calendar.setTime(convertedCurrentDate);
                    int monthprova = calendar.get(Calendar.MONTH)+1;
                    System.out.println(calendar.get(Calendar.YEAR) + " " + monthprova);
                    YearMonth yearMonthObject = YearMonth.of(calendar.get(Calendar.YEAR), monthprova);
                    double daysInMonth = yearMonthObject.lengthOfMonth();
                    SimpleDateFormat monthDate = new SimpleDateFormat("MMMM");
                    String month = monthDate.format(calendar.getTime());
                    String finMonth = month.substring(0,1).toUpperCase(Locale.ROOT)
                            +month.substring(1).toLowerCase(Locale.ROOT);
                    int meanVaxPerMonthArea = (int) (tuple._2/daysInMonth);
                    return finMonth+","+tuple._1._2()+","+meanVaxPerMonthArea;
                });

        HDFS_Utils.writeRDDToHdfs(HDFS_Utils.getOutputPathQuery1(), resultRDD);
        List<String> result = resultRDD.collect();
        Instant end = Instant.now();
        System.out.println("Query completed in " + Duration.between(start, end).toMillis() + " ms");
        CSV_Writer.query1CSV(result);
        sparkContext.close();
    }
}
