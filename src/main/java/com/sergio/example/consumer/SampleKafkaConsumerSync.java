package com.sergio.example.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * IMPORTANT: {@link KafkaConsumer} is not thread-safe. DON'T SHARE BETWEEN THREADS.
 */
public class SampleKafkaConsumerSync {

    public static void consume(String topic) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "first-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("max.partition.fetch.bytes", 1);
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // A rebalancing will be triggered when consumer is added or removed from consumer group
        consumer.subscribe(Collections.singletonList(topic) /*subscribe to a topic*/, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                // This method will be called before rebalancing starts and after the consumers
                // stops to consume messages.
                System.err.println("rebalancing is comming.");
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // This method will be called when after partitions has been assigned to the
                // broker but before the consumers start consume messages.
                System.err.println("rebalanced successful.");
            }

        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    System.err.println(Thread.currentThread().getName() + ": " + data);
                }
            }
        }finally {
            consumer.close();
        }
    }

}
