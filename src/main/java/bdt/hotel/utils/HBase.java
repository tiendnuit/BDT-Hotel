package bdt.hotel.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;


public class HBase {
	
	public static final String TABLE_NAME = "Hotel";
	
	public static final String CF_HOTEL= "CF_HOTEL";
	public static final String CF_HOTEL_hotel = "hotel";

	public static final String CF_BOOKING = "CF_BOOKING";
	public static final String CF_BOOKING_is_canceled = "is_canceled";
	public static final String CF_BOOKING_lead_time = "lead_time";
	public static final String CF_BOOKING_arrival_date_year = "arrival_date_year";
	public static final String CF_BOOKING_arrival_date_month = "arrival_date_month";
	public static final String CF_BOOKING_arrival_date_week_number = "arrival_date_week_number";
	public static final String CF_BOOKING_arrival_date_day_of_month_= "arrival_date_day_of_month";
	public static final String CF_BOOKING_stays_in_weekend_nights = "stays_in_weekend_nights";
	public static final String CF_BOOKING_stays_in_week_nights = "stays_in_week_nights";
	public static final String CF_BOOKING_adults_= "adults";
	public static final String CF_BOOKING_children = "children";
	public static final String CF_BOOKING_babies = "babies";
	public static final String CF_BOOKING_meal = "meal";

	public static final String CF_BOOKING_DETAILS = "CF_BOOKING_DETAILS";
	public static final String CF_BOOKING_DETAILS_country = "country";
	public static final String CF_BOOKING_DETAILS_market_segment = "market_segment";
	public static final String CF_BOOKING_DETAILS_distribution_channel = "distribution_channel";
	public static final String CF_BOOKING_DETAILS_is_repeated_guest = "is_repeated_guest";
	public static final String CF_BOOKING_DETAILS_previous_cancellations = "previous_cancellations";
	public static final String CF_BOOKING_DETAILS_previous_bookings_not_canceled = "previous_bookings_not_canceled";
	public static final String CF_BOOKING_DETAILS_reserved_room_type = "reserved_room_type";
	public static final String CF_BOOKING_DETAILS_assigned_room_type = "assigned_room_type";
	public static final String CF_BOOKING_DETAILS_booking_changes = "booking_changes";
	public static final String CF_BOOKING_DETAILS_deposit_type = "deposit_type";
	public static final String CF_BOOKING_DETAILS_agent = "agent";
	public static final String CF_BOOKING_DETAILS_company = "company";
	public static final String CF_BOOKING_DETAILS_days_in_waiting_list = "days_in_waiting_list";
	public static final String CF_BOOKING_DETAILS_customer_type = "customer_type";
	public static final String CF_BOOKING_DETAILS_adr = "adr";
	public static final String CF_BOOKING_DETAILS_required_car_parking_spaces = "required_car_parking_spaces";
	public static final String CF_BOOKING_DETAILS_total_of_special_requests = "total_of_special_requests";
	public static final String CF_BOOKING_DETAILS_reservation_status = "reservation_status";
	public static final String CF_BOOKING_DETAILS_reservation_status_date = "reservation_status_date";
	
	public static final String SQL_HOTEL = "hotel";
	public static final String SQL_BOOKING_IS_CANCEL = "is_canceled";
	public static final String SQL_BOOKING_AGENT = "agent";
	
	private static Configuration config;
	private static Connection connection;
	private static TableName tableName;
	private static long rowKey = 0;
	
	
	private static void setupDB() throws IOException  {
		config = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(config);
		tableName = TableName.valueOf(TABLE_NAME);
	}
	
	public static void createTable() throws IOException {
		setupDB();
		try (Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(tableName);
			table.addFamily(new HColumnDescriptor(CF_HOTEL).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_BOOKING).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_BOOKING_DETAILS).setCompressionType(Algorithm.NONE));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println(" Done!");
		}
		
		
	}
	
	public static void addData(List<String> values) throws IOException {
		Table table = connection.getTable(tableName);
		rowKey++;
		List<Put> puts = new ArrayList<>();
		
		Put p = new Put(Bytes.toBytes(String.valueOf(rowKey)));
		p.addColumn(Bytes.toBytes(CF_HOTEL), Bytes.toBytes(CF_HOTEL_hotel),Bytes.toBytes(values.get(0)));
		
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_is_canceled),Bytes.toBytes(values.get(1)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_lead_time),Bytes.toBytes(values.get(2)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_arrival_date_year),Bytes.toBytes(values.get(3)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_arrival_date_month),Bytes.toBytes(values.get(4)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_arrival_date_week_number),Bytes.toBytes(values.get(5)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_arrival_date_day_of_month_),Bytes.toBytes(values.get(6)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_stays_in_weekend_nights),Bytes.toBytes(values.get(7)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_stays_in_week_nights),Bytes.toBytes(values.get(8)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_adults_),Bytes.toBytes(values.get(9)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_children),Bytes.toBytes(values.get(10)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_babies),Bytes.toBytes(values.get(11)));
		p.addColumn(Bytes.toBytes(CF_BOOKING), Bytes.toBytes(CF_BOOKING_meal),Bytes.toBytes(values.get(12)));
		
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_country),Bytes.toBytes(values.get(13)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_market_segment),Bytes.toBytes(values.get(14)));
		
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_distribution_channel),Bytes.toBytes(values.get(15)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_is_repeated_guest),Bytes.toBytes(values.get(16)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_previous_cancellations),Bytes.toBytes(values.get(17)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_previous_bookings_not_canceled),Bytes.toBytes(values.get(18)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_reserved_room_type),Bytes.toBytes(values.get(19)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_assigned_room_type),Bytes.toBytes(values.get(20)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_booking_changes),Bytes.toBytes(values.get(21)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_deposit_type),Bytes.toBytes(values.get(22)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_agent),Bytes.toBytes(values.get(23)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_company),Bytes.toBytes(values.get(24)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_days_in_waiting_list),Bytes.toBytes(values.get(25)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_customer_type),Bytes.toBytes(values.get(26)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_adr),Bytes.toBytes(values.get(27)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_required_car_parking_spaces),Bytes.toBytes(values.get(28)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_total_of_special_requests),Bytes.toBytes(values.get(29)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_reservation_status),Bytes.toBytes(values.get(30)));
		p.addColumn(Bytes.toBytes(CF_BOOKING_DETAILS), Bytes.toBytes(CF_BOOKING_DETAILS_reservation_status_date),Bytes.toBytes(values.get(31)));
		
		
		puts.add(p);
		
		table.put(puts);
		table.close();
		System.out.println("Added data <rowKey>: " + rowKey + " successfully");
	}
	
	
}
