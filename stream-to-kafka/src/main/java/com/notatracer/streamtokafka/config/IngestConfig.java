package com.notatracer.streamtokafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@ConfigurationProperties(prefix = "ingest")
public class IngestConfig {
    private static final String REQUEST_TIMEOUT_MS_CONFIG = "1000";

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestConfig.class);

    private String bootstrapServers;
    private String requestTimeoutMsConfig;
    private String schemaRegistryUrl;
    private String sessionPath;

    @Autowired
    private KafkaConfig kafkaConfig;

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
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
//                io.confluent.kafkaConfig.serializers.KafkaAvroSerializer.class);
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

    public String getSessionPath() {
        return sessionPath;
    }

    public void setSessionPath(String sessionPath) {
        this.sessionPath = sessionPath;
    }

    //
//    @Bean
//    public KafkaPublishingListener kafkaPublishingListener(KafkaProducer<String, byte[]> kafkaProducer) {
//        return new KafkaPublishingListener();
//    }
}
