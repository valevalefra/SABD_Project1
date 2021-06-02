package query2;

import com.google.common.collect.Iterables;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import utility.CSV_Writer;
import utility.Date_Parser;
import utility.HDFS_Utils;
import utility.Tuple_Comparator3;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.StreamSupport;

public class Query2_Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        Tuple_Comparator3<String, String, String> compare =
                new Tuple_Comparator3<>(Comparator.<String>naturalOrder(),
                        Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());

        //retrieving dataset from HDFS
        JavaRDD<String> dataset2 = sc.textFile(HDFS_Utils.getDS2());

        JavaPairRDD<Tuple3<String, String, String>, Integer> dataset = Query2_Preprocessing.preprocessing(dataset2)
                .reduceByKey(Integer::sum);

        Instant start = Instant.now();

        //(yyyy-MM, fascia d'età, regione), (totale, data completa),
        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> groupedDataset = dataset
                .mapToPair(row -> {

                    Date convertedCurrentDate = Date_Parser.getConvertedDate(row._1._1(), "yyyy-MM-dd");
                    long day = convertedCurrentDate.getTime();
                    Calendar calendar = Date_Parser.getCalendar(convertedCurrentDate);
                    String month = Date_Parser.getDate(calendar, "yyyy-MM");

                    SimpleRegression sr = new SimpleRegression();
                    sr.addData((double) day, (double) row._2);
                    return new Tuple2<>(new Tuple3<>(month, row._1._2(), row._1._3()), sr);
                });
        groupedDataset.groupByKey()
                .filter(row -> StreamSupport.stream(row._2().spliterator(), false).count() > 2);

        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> reducedDataset = groupedDataset
                .reduceByKey((a, b) -> {
                    a.append(b);
                    return a;
                });

        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> sortedDataset = reducedDataset.sortByKey(compare, true);

        JavaRDD<Tuple2<Tuple2<String, String>, Tuple2<Integer, String>>> finalDataset = sortedDataset
                .map(row -> {

                    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM");
                    Date monthConv = sdf2.parse(row._1._1());
                    Calendar c = new GregorianCalendar(Locale.ITALIAN);
                    c.setTime(monthConv);
                    c.add(Calendar.MONTH, 1);
                    Date newMonth = c.getTime();
                    String newStr = sdf2.format(newMonth);
                    double newVax = row._2().predict((double) newMonth.getTime());
                    return new Tuple2<>(new Tuple2<>(newStr, row._1._2()), new Tuple2<>((int) newVax, row._1._3()));
                }).sortBy(row -> row._2._1(), false, 1);

        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>> data = finalDataset
                .mapToPair(row -> new Tuple2<>(new Tuple2<>(row._1._1(), row._1._2()), new Tuple2<>(row._2._1(), row._2._2())))
                .groupByKey();

        List<Tuple2<Tuple3<String, String, Integer>, String>> finalList = new ArrayList<>();
        for (Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>> line : data.collect()) {
            List<Tuple2<Integer, String>> list = IteratorUtils.toList(Iterables.limit(line._2(), 5).iterator());
            for (int i = 0; i < 5; i++) {
                finalList.add(new Tuple2<>(new Tuple3<>(line._1()._1(), line._1()._2(),
                        list.get(i)._1()), list.get(i)._2()));
            }
        }
        Tuple_Comparator3<String, String, Integer> comp = new Tuple_Comparator3<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Integer>reverseOrder());
        JavaRDD<String> resultRDD = sc.parallelize(finalList)
                .mapToPair(row -> new Tuple2<>(row._1(), row._2())).sortByKey(comp)
                .map(row -> {
                    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
                    SimpleDateFormat sdf2 = new SimpleDateFormat("d MMMM");
                    String s = row._1._1() + "-01";;
                    Date monthConv = sdf1.parse(s);
                    Calendar c = new GregorianCalendar(Locale.ITALIAN);
                    c.setTime(monthConv);
                    Date newMonth = c.getTime();
                    String newStr = sdf2.format(newMonth);
                    String str = newStr.substring(0, 2) +
                            newStr.substring(2, 3).toUpperCase(Locale.ROOT) +
                            newStr.substring(3).toLowerCase();
                    return str+","+row._1._2()+","+row._2()+","+row._1._3();
                });

        HDFS_Utils.writeRDDToHdfs(HDFS_Utils.getOutputPathQuery2(), resultRDD);
        List<String> result = resultRDD.collect();
        System.out.println(result);
        CSV_Writer.query2CSV(result);
        Instant end = Instant.now();
        System.out.println("Query 2 completed in " + Duration.between(start, end).toMillis() + " ms");


    }
}


