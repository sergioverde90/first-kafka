package com.sergio.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * IMPORTANT: {@link KafkaProducer} is thread-safe. Can be shared between threads.
 */
public class SampleKafkaProducerSync {

    public static void produce(String topic, String message) {

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
            // sync fashion
            Future<RecordMetadata> record = producer.send(new ProducerRecord<String, String>(topic, message));
            record.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}