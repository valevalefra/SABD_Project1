package utility;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import query3.Query3_Result;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CSV_Writer {
    private static final String QUERY1_CSV_FILE_PATH = "results/query1_output.csv";
    private static final String QUERY2_CSV_FILE_PATH = "results/query2_output.csv";
    private static final String QUERY3_KMEANS_CSV_FILE_PATH = "results/query3_kmeans_output.csv";
    private static final String QUERY3_BISECTINGKMEANS_CSV_FILE_PATH = "results/query3_bisectingkmeans_output.csv";

    public static List<String> clusteringCSV(List<Query3_Result> result, int control) {
        int k = 2;
        List<String> listRDD = new ArrayList<>();
        try {
            File csv;
            switch (control){
                case 0:
                    csv = new File(QUERY3_KMEANS_CSV_FILE_PATH);
                    break;
                case 1:
                    csv = new File(QUERY3_BISECTINGKMEANS_CSV_FILE_PATH);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + control);
            }

            if (!csv.exists()) {
                // creates the file if it does not exist
                csv.createNewFile();
            }
            // append set to false to overwrite existing version of the same file
            FileWriter writer = new FileWriter(csv, false);
            writer.append("K")
                    .append(";")
                    .append("Clusters")
                    .append(";")
                    .append("Evaluation")
                    .append(";")
                    .append("Processing Time")
                    .append("\n");
            writer.flush();

            for (Query3_Result res : result) {
                Dataset<Row> predictions = res.getDataset();
                double time = res.getTime();
                double eval = res.getEval();
                Dataset<Row> newDataset = predictions.withColumn("area(tot%)",
                        functions.struct("area", "total%"));
                //cluster -> area + total% list
                List<Row> list = newDataset.groupBy("prediction").agg(
                        functions.collect_list("area(tot%)").as("area(tot%)")).collectAsList();
                writer.append(String.valueOf(k))
                        .append(";");
                for (Row elem : list) {
                    listRDD.add(k + "," + elem.get(0)+ ":" + elem.getList(1) + "," + eval + "," + time);
                    writer.append(" ")
                            .append(String.valueOf(elem.get(0)))
                            .append(":")
                            .append(String.valueOf(elem.getList(1)));
                }
                writer.append(";")
                        .append(String.valueOf(eval))
                        .append(";")
                        .append(time+" ms")
                        .append("\n");
                writer.flush();
                k++;
            }
            writer.close();
            System.out.print(csv.getPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return listRDD;
    }

    public static void query2CSV(List<String> result){
        File csv = new File(QUERY2_CSV_FILE_PATH);
        try {
            if (!csv.exists()) {
                // creates the file if it does not exist
                csv.createNewFile();
            }
            // append set to false to overwrite existing version of the same file
            FileWriter writer = new FileWriter(csv, false);
            writer.append("Date")
                    .append(";")
                    .append("Age")
                    .append(";")
                    .append("Area")
                    .append(";")
                    .append("#Vaccinations")
                    .append("\n");
            writer.flush();

            for (String str: result){
                String[] res = str.split(",");
                writer.append(res[0])
                        .append(";")
                        .append(res[1])
                        .append(";")
                        .append(res[2])
                        .append(";")
                        .append(res[3])
                        .append("\n");
                writer.flush();
            }
        writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void query1CSV(List<String> result) {
        File csv = new File(QUERY1_CSV_FILE_PATH);
        try {
            if (!csv.exists()) {
                // creates the file if it does not exist
                csv.createNewFile();
            }
            // append set to false to overwrite existing version of the same file
            FileWriter writer = new FileWriter(csv, false);
            writer.append("Month")
                    .append(";")
                    .append("Area")
                    .append(";")
                    .append("AVG Vaccinations")
                    .append("\n");
            writer.flush();

            for (String str: result){
                String[] res = str.split(",");
                writer.append(res[0])
                        .append(";")
                        .append(res[1])
                        .append(";")
                        .append(String.valueOf(res[2]))
                        .append("\n");
                writer.flush();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
