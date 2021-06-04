package hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utility.HDFS_Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class HBase_Main {
    private static final String TABLE_QUERY1 = "Query1_hbase_table";
    private static final String TABLE_QUERY2 = "Query2_hbase_table";
    private static final String TABLE_QUERY3_KMEANS = "Query3_kmeans_hbase_table";
    private static final String TABLE_QUERY3_BISECTING = "Query3_bisecting_hbase_table";

    // Query 1 table structure
    private static final String TABLE_QUERY1_CF = "Administrations";
    private static final String TABLE_QUERY1_C1 = "AVG Vaccinations";

    // Query 2 table structure
    private static final String TABLE_QUERY2_CF = "Administrations";
    private static final String TABLE_QUERY2_C1 = "Age";
    private static final String TABLE_QUERY2_C2 = "#Vaccinations";

    // Query 3 table structure
    private static final String TABLE_QUERY3_CF = "Clusters";
    private static final String TABLE_QUERY3_C1 = "Clusters";
    private static final String TABLE_QUERY3_C2 = "Evaluation";
    private static final String TABLE_QUERY3_C3 = "Processing Time";

    public static void main(String[] args) throws ServiceException, IOException {

        HBase_Client client = new HBase_Client();

        System.out.println("Htable started!");
        System.out.println("Preparing environment...");
        // delete tables if they already exist
        if (client.exists(TABLE_QUERY1)) {
            client.dropTable(TABLE_QUERY1);
        }
        if (client.exists(TABLE_QUERY2)) {
            client.dropTable(TABLE_QUERY2);
        }
        if (client.exists(TABLE_QUERY3_KMEANS)) {
            client.dropTable(TABLE_QUERY3_KMEANS);
        }
        if (client.exists(TABLE_QUERY3_BISECTING)) {
            client.dropTable(TABLE_QUERY3_BISECTING);
        }

        System.out.println("Htable environment ready!");
        System.out.println("Creating tables...");
        // create tables
        client.createTable(TABLE_QUERY1, TABLE_QUERY1_CF);
        client.createTable(TABLE_QUERY2, TABLE_QUERY2_CF);
        client.createTable(TABLE_QUERY3_KMEANS, TABLE_QUERY3_CF);
        client.createTable(TABLE_QUERY3_BISECTING, TABLE_QUERY3_CF);


        System.out.println("Importing hdfs data to tables...");
        // import data to tables
        resultQuery1(client);
        resultQuery2(client);
        resultQuery3(client, new Path(HDFS_Utils.getOutputPathQuery3KMeans()));
        resultQuery3(client, new Path(HDFS_Utils.getOutputPathQuery3Bisecting()));

        System.out.println("-----------------------\nPrinting Query 1 table:");
        client.scanTable(TABLE_QUERY1, TABLE_QUERY1_CF, TABLE_QUERY1_C1);
        System.out.println("-----------------------\nPrinting Query 2 table:");
        client.scanTable(TABLE_QUERY2, TABLE_QUERY2_CF, TABLE_QUERY2_C1);
        client.scanTable(TABLE_QUERY2, TABLE_QUERY2_CF, TABLE_QUERY2_C2);

        System.out.println("-----------------------\nPrinting Query 3 kmeans table:");
        client.scanTable(TABLE_QUERY3_KMEANS, TABLE_QUERY3_CF, TABLE_QUERY3_C1);
        client.scanTable(TABLE_QUERY3_KMEANS, TABLE_QUERY3_CF, TABLE_QUERY3_C2);
        client.scanTable(TABLE_QUERY3_KMEANS, TABLE_QUERY3_CF, TABLE_QUERY3_C3);

        System.out.println("-----------------------\nPrinting Query 3 bisecting table:");
        client.scanTable(TABLE_QUERY3_BISECTING, TABLE_QUERY3_CF, TABLE_QUERY3_C1);
        client.scanTable(TABLE_QUERY3_BISECTING, TABLE_QUERY3_CF, TABLE_QUERY3_C2);
        client.scanTable(TABLE_QUERY3_BISECTING, TABLE_QUERY3_CF, TABLE_QUERY3_C3);

        client.closeConnection();
    }
    private static void resultQuery1(HBase_Client client) {
        String line;
        String key;
        String month;
        String area;
        String avg;

        Configuration configuration = new Configuration();

        try {
            FSDataInputStream inputStream;
            BufferedReader br;
            // get HDFS connection
            FileSystem hdfs = FileSystem.get(new URI(HDFS_Utils.getHdfs()), configuration);
            Path dirPath = new Path(HDFS_Utils.getOutputPathQuery1());
            FileStatus[] fileStatuses = hdfs.listStatus(dirPath);
            for (FileStatus fileStatus : fileStatuses) {
                // _SUCCESS file and subdirectories are ignored
                if (!fileStatus.isDirectory() && !fileStatus.getPath().toString().contains("SUCCESS")) {
                    inputStream = hdfs.open(fileStatus.getPath());
                    br = new BufferedReader(new InputStreamReader(inputStream));

                    while ((line = br.readLine()) != null) {
                        String[] str = line.split(",");
                        month = str[0];
                        area = str[1];
                        key = month+" "+area;
                        avg = str[2];
                        client.put(TABLE_QUERY1, key,
                                TABLE_QUERY1_CF, TABLE_QUERY1_C1, avg);

                    }
                    br.close();
                    inputStream.close();
                }
            }
            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not load query 1 result from HDFS");
        }
    }


    private static void resultQuery2(HBase_Client client) {
        String line;
        String key;
        String date;
        String age;
        String area;
        String numVax;

        Configuration configuration = new Configuration();

        try {
            FSDataInputStream inputStream;
            BufferedReader br;
            // get HDFS connection
            FileSystem hdfs = FileSystem.get(new URI(HDFS_Utils.getHdfs()), configuration);
            Path dirPath = new Path(HDFS_Utils.getOutputPathQuery2());
            FileStatus[] fileStatuses = hdfs.listStatus(dirPath);
            for (FileStatus fileStatus : fileStatuses) {
                // _SUCCESS file and subdirectories are ignored
                if (!fileStatus.isDirectory() && !fileStatus.getPath().toString().contains("SUCCESS")) {
                    inputStream = hdfs.open(fileStatus.getPath());
                    br = new BufferedReader(new InputStreamReader(inputStream));

                    while ((line = br.readLine()) != null) {
                        String[] str = line.split(",");
                        date = str[0];
                        age = str[1];
                        area = str[2];
                        numVax = str[3];
                        key = date+" "+area;
                        client.put(TABLE_QUERY2, key,
                                TABLE_QUERY2_CF, TABLE_QUERY2_C1, age,
                                TABLE_QUERY2_CF, TABLE_QUERY2_C2, numVax);
                    }
                    br.close();
                    inputStream.close();
                }
            }
            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not load query 2 result from HDFS");
        }
    }

    private static void resultQuery3(HBase_Client client, Path dirPath) {
        String line;
        String key;
        String k;
        String clusters;
        String evaluation;
        String processingTime;

        Configuration configuration = new Configuration();
        String nameTable;
        System.out.println(dirPath.getName());
        if(dirPath.getName().equals("query3KMeans")) nameTable = TABLE_QUERY3_KMEANS;
        else nameTable = TABLE_QUERY3_BISECTING;

        try {
            FSDataInputStream inputStream;
            BufferedReader br;
            // get HDFS connection
            FileSystem hdfs = FileSystem.get(new URI(HDFS_Utils.getHdfs()), configuration);
            FileStatus[] fileStatuses = hdfs.listStatus(dirPath);
            for (FileStatus fileStatus : fileStatuses) {
                // _SUCCESS file and subdirectories are ignored
                if (!fileStatus.isDirectory() && !fileStatus.getPath().toString().contains("SUCCESS")) {
                    inputStream = hdfs.open(fileStatus.getPath());
                    br = new BufferedReader(new InputStreamReader(inputStream));

                    while ((line = br.readLine()) != null) {
                        String[] str = line.split(",");
                        k = str[0];
                        clusters = str[1];
                        evaluation = str[2];
                        processingTime = str[3];
                        key = k;
                        client.put(nameTable, key,
                                TABLE_QUERY3_CF, TABLE_QUERY3_C1, clusters,
                                TABLE_QUERY3_CF, TABLE_QUERY3_C2, evaluation,
                                TABLE_QUERY3_CF, TABLE_QUERY3_C3, processingTime);
                    }
                    br.close();
                    inputStream.close();
                }
            }
            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not load query 2 result from HDFS");
        }


    }
}
