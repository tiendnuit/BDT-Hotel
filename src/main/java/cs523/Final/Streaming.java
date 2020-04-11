package cs523.Final;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;


public class Streaming {
	
	final static String TOPIC = "HotelBookingSummary";
	public static void main(String[] args) throws Exception
	{
		Map<String, Object> kafkaParams = new HashMap<>();
	    kafkaParams.put("bootstrap.servers", "localhost:9093");
	    kafkaParams.put("key.deserializer", StringDeserializer.class);
	    kafkaParams.put("value.deserializer", StringDeserializer.class);
	    kafkaParams.put("group.id", "group");
	    kafkaParams.put("auto.offset.reset", "latest");
	    kafkaParams.put("enable.auto.commit", false);
	    
		//Topic
		Collection<String> topics = Arrays.asList(TOPIC);

		//Config
		SparkConf sparkConf = new SparkConf()
		.setAppName("Streaming Kafka-Spark")
		.setMaster("local[2]");
		
	    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		
		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
	                    streamingContext,
	                    LocationStrategies.PreferConsistent(),
	                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
	            );

		stream.foreachRDD(rdd -> rdd.foreach(System.out::println));

		//SQLContext sqlContext =  new SQLContext(jsp);
		
		String schemaString = "name,empid,salary";
//ConsumerRecord(topic = testTopic, partition = 0, offset = 579106, CreateTime = 1586567977855, checksum = 644638194, serialized key size = -1, serialized value size = 12, key = null, value = good evening)
	    // Start the computation
	    streamingContext.start();
	    streamingContext.awaitTermination();
	}
}
