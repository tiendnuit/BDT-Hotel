package bdt.hotel.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class SparkSQLWritter {

	public static void writeToParquet(Dataset<Row> dataset, String tableNum) {
		dataset.write().saveAsTable(tableNum);
		//SparkSession spark = SparkSession.builder().appName(HBase.SQL_HOTEL).master("local[*]").getOrCreate(); 
		
		//spark.conf().set("spark.debug.maxToStringFields", 10000);
        //dataset.write().format("spark.sql.warehouse.dir").option("hive1", "hive1").save();
        
        //dataset.write().mode(SaveMode.Append).save("/home/cloudera/hive/warehouse/Parquet");

	}
	
	
	public static void writeToHive(SQLContext sqlContext, String tableName) {
		HiveDB.insertDataUseSQL(sqlContext, tableName);
		
	}
	
	
	
}
