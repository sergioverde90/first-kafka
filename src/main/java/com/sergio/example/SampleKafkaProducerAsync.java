package com.sergio.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class SampleKafkaProducerAsync {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // ACK's config is the most important flag when is important the message lost.
        // acks='0' => The producer will not wait for response from broker. The producer assume that message has been
        //             sent successfully
        // acks='1' => The producer waits until the broker leader has retrieved the message. If error occurred the
        //             producer can retry sending the message avoiding loss data. The message can still
        //             be lost if leader has crashed or has not still elected and no replica has the message.
        //             Can increases the latency.
        // acks='all' => The producer retrieves the successful response when the leader and all replicas have retrieved
        //             the message. This is the safest mode. The latency is high in this mode.
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // wait leader broker to get response
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            // async fashion
            System.out.println("sending message...");
            Future<RecordMetadata> record = producer.send(new ProducerRecord<>("topic1", "key1", "hello world! from Kafka"));
            System.out.println("message sent!");
            System.out.println("waiting response from kafka broker...");
            record.get();
            System.out.println("message has been retrieved successfully");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}