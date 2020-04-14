package bdt.hotel.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.SQLContext;



public class HiveDB {
	
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static Connection con;
	
	private static final String HiveSQLCreateTable = 
	"(	key string, "+
	  	"hotel string, "+
		"is_canceled int," +
		"lead_time int," +
		"arrival_date_year int," +
		"arrival_date_month string," +
		"arrival_date_week_number int," +
		"arrival_date_day_of_month int," +
		"stays_in_weekend_nights int," +
		"stays_in_week_nights int," +
		"adults int," +
		"children int," +
		"babies int," +
		"meal string," +
		"country string," +
		"market_segment string," +
		"distribution_channel string," +
		"is_repeated_guest int," +
		"previous_cancellations int," + 
		"previous_bookings_not_canceled int," +
		"reserved_room_type string," +
		"assigned_room_type string," +
		"booking_changes int," +
		"deposit_type string," +
		"agent int," +
		"company int," +
		"days_in_waiting_list int," +
		"customer_type string," +
		"adr double," +
		"required_car_parking_spaces int," +
		"total_of_special_requests int," +
		"reservation_status string," +
		"reservation_status_date date)"; 

	
	public static void viewDateUseSQL(SQLContext sqlContext, String tableName, int numLine) {
		sqlContext.sql("select * from " + tableName).show(numLine);
	}
	
	public static void createTableUseSQL(SQLContext sqlContext, String tableName) {
		//sqlContext.sql("create table h1 as select * from  " + tableName);
		sqlContext.sql("create table " + tableName + HiveSQLCreateTable);
	}
	
	public static void deleteTableUseSQL(SQLContext sqlContext, String tableName) {
		sqlContext.sql("truncate * from  " + tableName);
	}
	
	public static void insertDataUseSQL(SQLContext sqlContext, String tableName) {
		//sqlContext.sql("select * from hotel").show(100);
		//sqlContext.sql("create table h1 as select * from hotel");
		sqlContext.sql("insert into h1 select * from " + tableName);
	}
	
	
	
	public static void createTableUseJDBC(String tableName) throws ClassNotFoundException, SQLException{
		Class.forName(hiveDriverName);
		
	    // get connection
	    con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "cloudera", "cloudera");
	    Statement stmt = con.createStatement();
	    stmt.execute("select * from hotel");
	
	}
	
	public static void insertDataUseJDBC(String tableName, SQLContext sqlContext) throws ClassNotFoundException, SQLException {
	
		// Register driver and create driver instance
	    //Class.forName(hiveDriverName);
	
	    // get connection
	   // Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "cloudera", "cloudera");
	
	    // create statement
	    Statement stmt = con.createStatement();
	    
	    StringBuilder sb = new StringBuilder();
	    
		sb.append("insert into " + tableName);
		sb.append("values( " + "1");
		    
		    
	    stmt.execute(sb.toString());
	   
	    //con.close();
		}
	
	
	
	
}


