package hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An hbase client containing the required functions to export data from HDFS to the datastore
 * In order to use this class make sure to have in /etc/hosts the line "127.0.0.1  hbase".
 */

public class HBase_Client {

    private static final String ZOOKEEPER_HOST = "hbase";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String HBASE_MASTER = "hbase:16000";
    private static final int HBASE_MAX_VERSIONS = 1;

    private static final boolean DEBUG = true;

    private Connection connection = null;

    private static byte[] b(String s){
        return Bytes.toBytes(s);
    }

    /**
     * Create a connection with HBase
     * @return Connection
     * @throws IOException
     * @throws ServiceException
     */
    public Connection getConnection() throws IOException, ServiceException {

        if (!(connection == null || connection.isClosed() || connection.isAborted()))
            return connection;

//        if (!DEBUG)
        Logger.getRootLogger().setLevel(Level.ERROR);

        Configuration conf  = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_HOST);
        conf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
        conf.set("hbase.master", HBASE_MASTER);

        /* Check configuration */
        HBaseAdmin.checkHBaseAvailable(conf);

        if (DEBUG)
            System.out.println("HBase is running!");

        this.connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    /* *******************************************************************************
     *  Database administration
     * ******************************************************************************* */

    /**
     * Create a new table named tableName, with the specified columnFamilies
     *
     * @param tableName
     * @param columnFamilies
     * @return
     */
    public boolean createTable(String tableName, String... columnFamilies) {

        try {

            Admin admin = getConnection().getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(
                    TableName.valueOf(tableName));


            for (String columnFamily : columnFamilies) {
                HColumnDescriptor cd = new HColumnDescriptor(columnFamily);
                cd.setMaxVersions(HBASE_MAX_VERSIONS);
                tableDescriptor.addFamily(cd);
            }

            admin.createTable(tableDescriptor);
            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;

    }

    /**
     * Check if a table exists
     * @param table table name
     * @return  true if a table exists
     */
    public boolean exists(String table){

        try {

            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);
            return admin.tableExists(tableName);

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;

    }

    /**
     * Drop a table and all its content
     *
     * @param table table to be deleted
     * @return true if a table has been deleted
     */
    public boolean dropTable(String table) {

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);

            // To delete a table or change its settings, you need to first disable the table
            admin.disableTable(tableName);

            // Delete the table
            admin.deleteTable(tableName);

            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;
    }


    /* *******************************************************************************
     *  CRUD operations
     * ******************************************************************************* */

    /**
     * Insert a new row in a table.
     *
     * To put a record, this method should be executed by providing triples of
     *  columnFamily, column, value
     *
     * If a columnFamily:column already exists, the value is updated.
     * Values are stacked according to their timestamp.
     *
     * @param table     table name
     * @param rowKey    row name
     * @param columns   columnFamily, column, value
     * @return          true if the record is inserted
     */
    public boolean put(String table, String rowKey, String... columns){

        if (columns == null || (columns.length % 3 != 0)) {
            // Invalid usage of the function; columns should contain 3-ple in the
            // following format:
            // - columnFamily
            // - column
            // - value
            return false;
        }

        try {

            Table hTable = getConnection().getTable(TableName.valueOf(table));

            Put p = new Put(b(rowKey));

            for (int i = 0; i < (columns.length / 3); i++){

                String columnFamily = columns[i * 3];
                String column       = columns[i * 3 + 1];
                String value        = columns[i * 3 + 2];
                p.addColumn(b(columnFamily), b(column), b(value));

            }

            // Saving the put Instance to the HTable.
            hTable.put(p);

            // closing HTable
            hTable.close();

            return true;
        } catch (IOException | ServiceException e) {
//            e.printStackTrace();
        }

        return false;
    }


    /**
     * Scan the content of the table.
     *
     * @param table            table to scan
     * @param columnFamily     columnFamily to scan
     * @param column           column to scan
     * @throws IOException
     * @throws ServiceException
     */
    public void scanTable(String table, String columnFamily, String column) throws IOException, ServiceException {

        Table products = getConnection().getTable(TableName.valueOf(table));

        Scan scan = new Scan();

        if (columnFamily != null && column != null)
            scan.addColumn(b(columnFamily), b(column));

        else if (columnFamily != null)
            scan.addFamily(b(columnFamily));

        ResultScanner scanner = products.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            System.out.println("Found row : " + result);
        }

        scanner.close();

    }

    public void closeConnection() {

        if (!(this.connection == null || this.connection.isClosed() || this.connection.isAborted())) {
            try {
                this.connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                this.connection = null;
            }
        }
    }

}