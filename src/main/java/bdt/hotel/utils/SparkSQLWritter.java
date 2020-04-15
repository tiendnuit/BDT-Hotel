package bdt.hotel.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class SparkSQLWritter {

	public static void writeToParquet(Dataset<Row> dataset, String tableNum) {
		dataset.write().saveAsTable(tableNum);
		
	}
	
	
	public static void writeToHive(SQLContext sqlContext, String tableName, String sparkTempViewName) {
		HiveDB.insertDataUseSQL(sqlContext, tableName, sparkTempViewName);
		
	}
	
	
	
}
