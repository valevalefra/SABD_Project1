package query2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query2_Preprocessing {
    public static JavaPairRDD<Tuple3<String, String, String>, Integer> preprocessing(JavaRDD<String> dataset2) {

        String firstLine = dataset2.first();
        JavaPairRDD<Tuple3<String, String, String>, Integer> filterdataset = dataset2.
                filter(row-> row.split(",")[0].compareTo("2021-02-01")>=0 && !(row.equals(firstLine)))
                .mapToPair( row -> {
                    String[] lineSplit = row.split(",");
                    return new Tuple2<>(new Tuple3<>(lineSplit[0], lineSplit[3], lineSplit[11]), Integer.parseInt(lineSplit[5]));
        });

       // Map<Tuple3<String, String, String>, Iterable<Integer>> map = finalDataset.collectAsMap();
                //.flatMap(
                //row -> {
                    //List<Tuple2<Tuple3<String, String, String>, String>> result = new ArrayList<>();
                    /*List<String> list = new ArrayList<>();
                    list.add(row.split(",")[0] + "," + row.split(",")[3] + "," +
                            row.split(",")[5] + "," + row.split(",")[21]);
                    return list.iterator();*/
              //  }

       // );


       return filterdataset;
       /*for(Tuple2<Tuple3<String, String, String>, Iterable<Tuple2<Integer, String>>> line:finalDataset.take(50)){
            System.out.println("* "+line);}*/

    }
}
