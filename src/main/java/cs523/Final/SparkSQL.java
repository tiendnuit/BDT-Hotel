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
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.apache.spark.sql.hive.HiveContext;

import bdt.hotel.utils.HBase;
import bdt.hotel.utils.HiveDB;
import bdt.hotel.utils.ParseFile;

import com.google.protobuf.ServiceException;

public class SparkSQL {

	private static Configuration config;
	@SuppressWarnings("unused")
	private static Connection connection;
	private static String tableStringName = HBase.TABLE_NAME;
	@SuppressWarnings("unused")
	private static TableName tableName;
	
	
	private static void setupDB() throws IOException  {
		config = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(config);
		tableName = TableName.valueOf(tableStringName);
	}
	
	@SuppressWarnings("deprecation")
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
		//Spec config Config loading
		SparkConf sparkConf = new SparkConf()
				.setAppName(HBase.SQL_HOTEL)
				
				.setMaster("local[2]")
				
				.set("spark.sql.warehouse.dir", "/home/cloudera/hive/warehouse");
				
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		//context.hadoopConfiguration().set("spark.hbase.host", "localhost");
		//context.hadoopConfiguration().set("spark.hbase.port", "2181");
        //SQLContext sqlContext = new SQLContext(context);
		SQLContext sqlContext = new SQLContext(context);
        
        
        //SparkSession sparkSession = SparkSession.builder().master("local[2]").appName(HBase.SQL_HOTEL)
   		//	 .config("spark.sql.warehouse.dir", "/home/cloudera/hive/warehouse").getOrCreate();
        
        String catalogSQLScheme = ParseFile.getAllLinesFromJson("/home/cloudera/git/BDT-Hotel/src/main/java/bdt/hotel/json/HotelSQLSchema.json");
        
        // Check for Hotel SQL schema
        System.out.println(catalogSQLScheme);
        
        //Mapping json schema 
        Map<String, String> optionsMap = new HashMap<>();
        String htc = HBaseTableCatalog.tableCatalog();
        optionsMap.put(htc, catalogSQLScheme);
        
        //create dataframe and temporary views table by Spark SQL
        Dataset<Row> dataset = sqlContext.read().options(optionsMap).format("org.apache.spark.sql.execution.datasources.hbase").load();
        dataset.createOrReplaceTempView(tableStringName + "_mem");
        
        //test to display table in console
        //HiveDB.viewDateUseSQL(sqlContext, tableStringName + "_mem", 20);
        
        //create table Hive
        HiveDB.createTableUseSQL(sqlContext, "tabletest1");
        //insert data
        //HiveDB.insertDataUseSQL(sqlContext);
        //HiveDB.createTableUseJDBC("testtest");
        
       
        
        System.out.println("done to query");
     
	}
	
	
	
	public static void main(String[] args) {
		try {
			SparkSQL.readData();
		} catch (IOException | ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

	}

}
