#!/bin/sh
pwd
Kafka_location="/home/cloudera/Downloads/kafka_2.12-2.4.1"
cd $Kafka_location

# start broker and run producer Kafka
bin/kafka-server-start.sh config/server.properties &
bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic HotelBookingSummary --replication-factor 1 --partitions 1
/home/cloudera/git/BDT-Hotel/script/runPushData.sh
