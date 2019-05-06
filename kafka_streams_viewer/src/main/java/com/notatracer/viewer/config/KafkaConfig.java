package com.notatracer.viewer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

@Validated
@Component
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfig {

    @NotNull
    private String topic;

    @NotNull
    private int numPartitions;

    @NotNull
    private String bootstrapServers;

    private String requestTimeoutMsConfig;


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getRequestTimeoutMsConfig() {
        return requestTimeoutMsConfig;
    }

    public void setRequestTimeoutMsConfig(String requestTimeoutMsConfig) {
        this.requestTimeoutMsConfig = requestTimeoutMsConfig;
    }
}
