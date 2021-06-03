# SABD_Project1

  ###Project description
  
   Below is given a brief description of the folders that make up the project.
   
   ####docker
   
   This folder contains scripts and xml files to manage containers. A more accurate description 
   can be found in a specific README present in the docker folder.
   
   ####results
   In this folder can be found all the outputs relative to the required queries. In particular:
   * *query1_output.csv*, contains query1 results; 
   * *query2_out.csv*, contains query2 results;
   * *query3_bisectingkmeans_output.csv* contains query3 results for the Bisecting K-Means algorithm;
   * *query3_kmeans_output.csv* contains query3 results for the K-Means algorithm;
   
   ####src/main/java
   Here is contained the main Java code for the project. In particular it is divided into five folders:
   * **hbase** rapresenting the code for the storage system to export data from HDFS. This is further divided into:
     * *HBase_Client.java* contains methods to implement HBase client;
     * *HBase_Main.java* contains methods to export data from HDFS to HBase.
   * **query1** containing the code for the query 1 implementation. It's divided in:
     * *Query1_Main.java* contains the query 1 resolution idea implementation; 
     * *Query1_Preprocessing.java* contains the query 1 preprocessing.
   * **query2** containing the code for the query 2 implementation. It's divided in:  
     * *Query2_Main.java* contains the query 2 processing;
     * *Query2_Preprocessing.java* contains the query 2 preprocessing.
   * **query3** containing the code for the query 3 implementation. It's divided in:
     * *Query3_Main.java* implements query 3 resolution;
     * *Query3_Preprocessing.java* contains query 3 preprocessing;
     * *Query3_Result.java* represents an object useful for clustering implementation.
   * **utility** containing utilities for the queries computation. It's composed by:
     * *Clustering_Utils.java* implements clustering algorithm. In particular comprehends the implementation either for KMeans and for Bisecting KMeans;
     * *CSV_Writer.java* contains methods allowing writing on csv;
     * *Date_Parser.java* contains methods for dates managing;
     * *HDFS_Utils.java* allows interaction with HDFS;
     * *Tuple_Comparator2.java* and *Tuple_Comparator3.java* represent structures useful for sorting operations in RDDs.  
   
