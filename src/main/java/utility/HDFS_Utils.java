package utility;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.sparkproject.dmg.pmml.DataType;
import scala.Tuple3;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class HDFS_Utils {
    private final static String HDFS_NAMENODE_ADDRESS = "127.0.0.1";
    private final static String HDFS_NAMENODE_PORT = "9871";
    private final static String INPUT_FOLDER = "/data";
    private final static String OUTPUT_FOLDER = "/output";
    private final static String DATASET1_FILENAME = "/DS1.csv";
    private final static String DATASET2_FILENAME = "/DS2.csv";
    private final static String DATASET3_FILENAME = "/DS3.csv";
    private final static String DATASET4_FILENAME = "/totalPopulation.csv";
    private final static String QUERY1_RESULT = "/query1";
    private final static String QUERY2_RESULT = "/query2";
    private final static String QUERY3_RESULT_KMEANS = "/query3KMeans";
    private final static String QUERY3_RESULT_BISECTING = "/query3Bisecting";

    public static String getDS1() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + INPUT_FOLDER + DATASET1_FILENAME;
    }

    public static String getDS2() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + INPUT_FOLDER + DATASET2_FILENAME;
    }

    public static String getDS3() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + INPUT_FOLDER + DATASET3_FILENAME;
    }

    public static String getTotPopulation() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + INPUT_FOLDER + DATASET4_FILENAME;
    }

    public static String getOutputPathQuery1() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY1_RESULT;
    }

    public static String getOutputPathQuery2() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY2_RESULT;
    }

    public static String getOutputPathQuery3KMeans() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY3_RESULT_KMEANS;
    }
    public static String getOutputPathQuery3Bisecting() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT + OUTPUT_FOLDER + QUERY3_RESULT_BISECTING;
    }

    public static String getHdfs() {
        return "hdfs://" + HDFS_NAMENODE_ADDRESS + ":" + HDFS_NAMENODE_PORT;
    }

    public static void writeRDDToHdfs(String path, JavaRDD rdd) {
        Configuration configuration = new Configuration();
        try {
            // connect to HDFS
            FileSystem hdfs = FileSystem.get(new URI(HDFS_Utils.getHdfs()), configuration);
            // delete previous file version
            hdfs.delete(new Path(path), true);
            hdfs.close();
            // save new file version
            rdd.saveAsTextFile(path);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not save file to HDFS");
        }
    }
}

