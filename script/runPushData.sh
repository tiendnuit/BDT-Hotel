#sudo sh /home/cloudera/git/BDT-Hotel1/script/pushData | /home/cloudera/Desktop/Final/kafka_2.11-1.0.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic HotelBookingSummary


sudo sh /home/cloudera/git/BDT-Hotel/script/pushData.sh | /home/cloudera/Downloads/kafka_2.12-2.4.1/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic HotelBookingSummary
