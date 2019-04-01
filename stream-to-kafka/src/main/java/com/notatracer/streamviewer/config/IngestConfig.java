package com.notatracer.streamviewer.config;

import com.notatracer.streamviewer.stream.reader.KafkaPublishingListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Configuration
@ConfigurationProperties
public class IngestConfig {
    private static final String REQUEST_TIMEOUT_MS_CONFIG = "1000";

    private String bootstrapServers = "localhost:9092";
    private String requestTimeoutMsConfig = "3000";
    private String schemaRegistryUrl = "http://localhost:8081";


    private Kafka kafka;

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getRequestTimeoutMsConfig() {
        return this.requestTimeoutMsConfig;
    }

    public void setRequestTimeoutMsConfig(String requestTimeoutMsConfig) {
        this.requestTimeoutMsConfig = requestTimeoutMsConfig;
    }

    public String getSchemaRegistryUrl() {
        return this.schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Bean
    public KafkaProducer<String, byte[]> kafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class);
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
//                TimePartitioner.class.getCanonicalName())
//        props.put("schema.registry.url", properties.getSchemaRegistryUrl());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        return new KafkaProducer<String, byte[]>(props);
    }
//
//    @Bean
//    public KafkaPublishingListener kafkaPublishingListener(KafkaProducer<String, byte[]> kafkaProducer) {
//        return new KafkaPublishingListener();
//    }
}
