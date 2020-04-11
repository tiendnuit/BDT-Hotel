package cs523.Final;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import bdt.hotel.utils.HBase;


public class Streaming {
	
	final static String TOPIC = "HotelBookingSummary1";
	public static void main(String[] args) throws Exception
	{
		Map<String, Object> kafkaParams = new HashMap<>();
	    kafkaParams.put("bootstrap.servers", "localhost:9092");
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

		
		//Create HBase table
		HBase.createTable();
		//Put data for each row from Kafka
		
		stream.foreachRDD(rdd -> rdd.foreach(x ->  writeValue(x.value())));

		//SQLContext sqlContext =  new SQLContext(jsp);
		
	    // Start the computation
	    streamingContext.start();
	    streamingContext.awaitTermination();
	}
	
	private static void writeValue(String record){
		List<String> fieldValues = Arrays.asList(record.split(","));
		try {
			HBase.addData(fieldValues);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
