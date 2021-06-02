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

    private static final int TOP = 5;

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
        JavaRDD<String> initialRdd = sc.textFile(HDFS_Utils.getDS2());

        //reducing by key the preprocessed rdd in order to sum all the number of female vaccinations
        //for specific date, age and area
        JavaPairRDD<Tuple3<String, String, String>, Integer> rdd = Query2_Preprocessing.preprocessing(initialRdd)
                .reduceByKey(Integer::sum);

        Instant start = Instant.now();


        //JavaPairRDD in which each row consists of a tuple (yyyy-MM, age, region), to whom
        //is associated the SimpleRegression observation, consisting of the couple
        //(day, number of vaccinations per day)
        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> groupedRdd = rdd
                .mapToPair(row -> {

                    Date convertedCurrentDate = Date_Parser.getConvertedDate(row._1._1(), "yyyy-MM-dd");
                    long day = convertedCurrentDate.getTime();
                    Calendar calendar = Date_Parser.getCalendar(convertedCurrentDate);
                    String month = Date_Parser.getDate(calendar, "yyyy-MM");

                    //SimpleRegression observation
                    SimpleRegression sr = new SimpleRegression();
                    sr.addData((double) day, (double) row._2);
                    return new Tuple2<>(new Tuple3<>(month, row._1._2(), row._1._3()), sr);
                }).cache();

        //the obtained rdd is then filtered, taking to account the list length obtained after grouping by,
        //in such a way to have at least two days per month of vaccinations
        groupedRdd.groupByKey()
                .filter(row -> StreamSupport.stream(row._2().spliterator(), false).count() >= 2);

        //the obtained result is then reduced by key, in order to append data regarding SimpleRegression
        //observations for the same month, age and region. The rdd is then sorted by key.
        JavaPairRDD<Tuple3<String, String, String>, SimpleRegression> reducedRdd = groupedRdd
                .reduceByKey((a, b) -> {
                    a.append(b);
                    return a;
                }).sortByKey(compare, true);

        //Map operation in which it is performed the number of vaccinations prediction for the
        //1st of the month next to the one considered. The prediction is applied with respect
        //to the observations accumulated for a certain month.
        JavaRDD<Tuple2<Tuple2<String, String>, Tuple2<Integer, String>>> finalRdd = reducedRdd
                .map(row -> {
                    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM");
                    Date monthConv = sdf2.parse(row._1._1());
                    Calendar c = Date_Parser.getCalendar(monthConv);
                    //considering the 1st of month next to monthConv
                    c.add(Calendar.MONTH, 1);
                    Date newMonth = c.getTime();
                    String newStr = sdf2.format(newMonth);
                    //prediction done by taking into account the appended SampleRegression observations
                    //for a certain month and evaluating the predicted number of vaccinations by
                    //considering the numerical value of the considered date.
                    double newVax = row._2().predict((double) newMonth.getTime());
                    return new Tuple2<>(new Tuple2<>(newStr, row._1._2()), new Tuple2<>((int) newVax, row._1._3()));
                    //then the rdd is sorted by descending number of vaccinations
                }).sortBy(row -> row._2._1(), false, 1);

        //The obtained rdd is then grouped by the key (month, age), in such a way to preserve the
        //previously sorted vaccinations values
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>> data = finalRdd
                .mapToPair(row -> new Tuple2<>(new Tuple2<>(row._1._1(), row._1._2()), new Tuple2<>(row._2._1(), row._2._2())))
                .groupByKey();

        //collect action is performed in order to select top 5 vaccinations regions, relatively to a certain
        //month and age
        List<Tuple2<Tuple3<String, String, Integer>, String>> finalList = new ArrayList<>();
        for (Tuple2<Tuple2<String, String>, Iterable<Tuple2<Integer, String>>> line : data.collect()) {
            List<Tuple2<Integer, String>> list = IteratorUtils.toList(Iterables.limit(line._2(), TOP).iterator());
            for (int i = 0; i < TOP; i++) {
                finalList.add(new Tuple2<>(new Tuple3<>(line._1()._1(), line._1()._2(),
                        list.get(i)._1()), list.get(i)._2()));
            }
        }

        Tuple_Comparator3<String, String, Integer> comp =
                new Tuple_Comparator3<>(Comparator.<String>naturalOrder(),
                        Comparator.<String>naturalOrder(), Comparator.<Integer>reverseOrder());

        //required rdd obtained from the previously adjusted list
        JavaRDD<String> resultRDD = sc.parallelize(finalList)
                .mapToPair(row -> new Tuple2<>(row._1(), row._2())).sortByKey(comp)
                .map(row -> {
                    Date monthConv = Date_Parser.getConvertedDate(row._1._1() + "-01", "yyyy-MM-dd");
                    Calendar c = Date_Parser.getCalendar(monthConv);
                    String newStr = Date_Parser.getDate(c, "d MMMM");
                    String str = newStr.substring(0, 2) +
                            newStr.substring(2, 3).toUpperCase(Locale.ROOT) +
                            newStr.substring(3).toLowerCase();
                    return str+","+row._1._2()+","+row._2()+","+row._1._3();
                });

        HDFS_Utils.writeRDDToHdfs(HDFS_Utils.getOutputPathQuery2(), resultRDD);

        //collect action to obtain the final list
        List<String> result = resultRDD.collect();

        CSV_Writer.query2CSV(result);
        Instant end = Instant.now();
        System.out.println("Query 2 completed in " + Duration.between(start, end).toMillis() + " ms");


    }
}


