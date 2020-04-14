-- Code scala to display table information from HBase
%spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory,HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.SparkSession
def catalog =
      s"""{
         |“table”:{“namespace”:“default”, “name”:“Hotel”},
         |“rowkey”:“key”,
         |“columns”:{
         |“key”:{“cf”:“rowkey”, “col”:“key”, “type”:“string”},
         |“hotel”:{“cf”:“CF_HOTEL”, “col”:“hotel”, “type”:“string”},
         |“is_canceled”:{“cf”:“CF_BOOKING”, “col”:“is_canceled”, “type”:“string”},
         |“agent”:{“cf”:“CF_BOOKING_DETAILS”, “col”:“agent”, “type”:“string”}
         |}
         |}""".stripMargin
val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("HotelBookingSummary1")
      .getOrCreate()
import spark.implicits._
// Reading from HBase to DataFrame
val hbaseDF = spark.read
  .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
  .format("org.apache.spark.sql.execution.datasources.hbase")
  .load()
//Display Schema from DataFrame
hbaseDF.printSchema()
//Collect and show Data from DataFrame
hbaseDF.show(false)
//Create Temporary Table on DataFrame
hbaseDF.createOrReplaceTempView("hotel")
//Run SQL
spark.sql("select * from hotel limit 10").show