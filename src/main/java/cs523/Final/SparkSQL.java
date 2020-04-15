package cs523.Final;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import bdt.hotel.utils.Constants;
import bdt.hotel.utils.HBase;
import bdt.hotel.utils.HiveDB;
import bdt.hotel.utils.ParseFile;

import com.google.protobuf.ServiceException;

public class SparkSQL {

	public static final String SPARK_SQL_WAREHOUSE_DIR = "spark.sql.warehouse.dir";
	public static final String SPARK_SQL_WAREHOUSE_LOCATION = "/user/hive/warehouse";
	public static final String HBASE_URL = "org.apache.spark.sql.execution.datasources.hbase";
	public static final String JSON_FILE = "/home/cloudera/git/BDT-Hotel/src/main/java/bdt/hotel/json/HotelSQLSchema.json";
	public static final String HIVE_CONNECTOR_METADATA = "hive.metastore.uris"; //The property hive.metastore.uris is a comma separated list of metastore URIs run THRIFT ODBC/JDBC
	public static final String HIVE_CONNECTOR_THRIFT_URL = "thrift://quickstart.cloudera:9083";
	private static final String TABLE_TEMP_VIEW_SPARK_SQL = "hotel_mem";
	
	private static Configuration config;
	@SuppressWarnings("unused")
	private static Connection connection;
	private static final String tableStringName = HBase.TABLE_NAME;
	@SuppressWarnings("unused")
	private static TableName tableName;
	
	
	private static void setupDB() throws IOException  {
		config = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(config);
		tableName = TableName.valueOf(tableStringName);
	}
	
	public static void readData() throws IOException, ClassNotFoundException, SQLException {
		setupDB();
		//config.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		try {
	        HBaseAdmin.checkHBaseAvailable(config);
	        System.out.println("HBase is running");
	    } catch (ServiceException e) {
	        System.out.println("HBase is not running");
	        e.printStackTrace();
	    }
		
		
        SparkSession ss = SparkSession
        		.builder()
        		.appName(HBase.SQL_HOTEL)
        		.master(Constants.MASTER_RUN)
        		.config(SPARK_SQL_WAREHOUSE_DIR, SPARK_SQL_WAREHOUSE_LOCATION) 
        		.config(HIVE_CONNECTOR_METADATA, HIVE_CONNECTOR_THRIFT_URL) 
        		.enableHiveSupport()
        		.getOrCreate();
        SQLContext sqlContext = SQLContext.getOrCreate(ss.sparkContext());

        String catalogSQLScheme = ParseFile.getAllLinesFromJson(JSON_FILE);

        //Mapping json schema 
        Map<String, String> optionsMap = new HashMap<>();
        String htc = HBaseTableCatalog.tableCatalog();
        optionsMap.put(htc, catalogSQLScheme);
        
        //Load data from Hbase
        Dataset<Row> dataset = sqlContext.read().options(optionsMap).format(HBASE_URL).load();
        dataset.createOrReplaceTempView(TABLE_TEMP_VIEW_SPARK_SQL);
        System.out.println("Spark SQL running..");
       // ============ DEMO SPARKSQL to Hive using SQL
       //Load data set and create table Spark by SQL
       HiveDB.dropTableUseSQL(sqlContext, HiveDB.HIVE_TABLE_NAME);
       
       //Create table use Spark SQL
       HiveDB.createTableBySelectUseSQL(sqlContext,  HiveDB.HIVE_TABLE_NAME, TABLE_TEMP_VIEW_SPARK_SQL); 
       
       //Insert data
        //HiveDB.truncateTableUseSQL(sqlContext, HiveDB.HIVE_TABLE_NAME);
        HiveDB.insertDataUseSQL(sqlContext, HiveDB.HIVE_TABLE_NAME, TABLE_TEMP_VIEW_SPARK_SQL);
       
	}
	
	
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		try {
			SparkSQL.readData();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}