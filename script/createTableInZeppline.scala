%spark
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
sqlContext.sql("DROP TABLE IF EXISTS hotel")

def sqlStatement =		 
s"""CREATE EXTERNAL TABLE hotel
 |(	key string,
 | 	hotel string,
 |	is_canceled int,
 |	lead_time int,
 |	arrival_date_year int,
 |	arrival_date_month string,
 |	arrival_date_week_number int,
 |	arrival_date_day_of_month int,
 |	stays_in_weekend_nights int,
 |	stays_in_week_nights int,
 |	adults int,
 |	children int,
 |	babies int,
 |	meal string,
 |	country string,
 |	market_segment string,
 |	distribution_channel string,
 |	is_repeated_guest int,
 |	previous_cancellations int, 
 |	previous_bookings_not_canceled int,
 |	reserved_room_type string,
 |	assigned_room_type string,
 |	booking_changes int,
 |	deposit_type string,
 |	agent int,
 |	company int,
 |	days_in_waiting_list int,
 |	customer_type string,
 |	adr double,
 |	required_car_parking_spaces int,
 |	total_of_special_requests int,
 |	reservation_status string,
 |	reservation_status_date date
 |
|) 
|STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
|WITH SERDEPROPERTIES 
|("hbase.columns.mapping" = ":key,CF_HOTEL:hotel,
|								
|								CF_BOOKING:is_canceled,
|								CF_BOOKING:lead_time,
|								CF_BOOKING:arrival_date_year,
|								CF_BOOKING:arrival_date_month,
|								CF_BOOKING:arrival_date_week_number,
|								CF_BOOKING:arrival_date_day_of_month,
|								CF_BOOKING:stays_in_weekend_nights,
|								CF_BOOKING:stays_in_week_nights,
|								CF_BOOKING:adults,
|								CF_BOOKING:children,
|								CF_BOOKING:babies,
|								CF_BOOKING:meal,
|								
|								CF_BOOKING_DETAILS:country,
|								CF_BOOKING_DETAILS:market_segment,
|								CF_BOOKING_DETAILS:distribution_channel,
|								CF_BOOKING_DETAILS:is_repeated_guest,
|								CF_BOOKING_DETAILS:previous_cancellations,
|								CF_BOOKING_DETAILS:previous_bookings_not_canceled,
|								CF_BOOKING_DETAILS:reserved_room_type,
|								CF_BOOKING_DETAILS:assigned_room_type,
|								CF_BOOKING_DETAILS:booking_changes,
|								CF_BOOKING_DETAILS:deposit_type,
|								CF_BOOKING_DETAILS:agent,
|								CF_BOOKING_DETAILS:company,
|								CF_BOOKING_DETAILS:days_in_waiting_list,
|								CF_BOOKING_DETAILS:customer_type,
|								CF_BOOKING_DETAILS:adr,
|								CF_BOOKING_DETAILS:required_car_parking_spaces,
|								CF_BOOKING_DETAILS:total_of_special_requests,
|								CF_BOOKING_DETAILS:reservation_status,
|								CF_BOOKING_DETAILS:reservation_status_date
|								")
|TBLPROPERTIES ("hbase.table.name" = "Hotel")""".stripMargin
