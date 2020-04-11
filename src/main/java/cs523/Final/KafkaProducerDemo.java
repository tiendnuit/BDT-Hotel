package cs523.Final;

public class KafkaProducerDemo {
    public static final String TOPIC = "testTopic";
    
    public static void main(String[] args) {
        boolean isAsync = false;
        Producer producerThread = new Producer(TOPIC, isAsync);
        // start the producer
        producerThread.start();
 
    }
}
