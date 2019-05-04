package com.notatracer.kafka.util;

import com.notatracer.viewer.config.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaUtil.class);

    public static boolean topicPartitionExists(String topic, int partitionNum, KafkaConfig kafkaConfig) {
        LOGGER.info(String.format("Check if TopicPartition exists [topic=%s, partition=%d]", topic, partitionNum));

        AdminClient adminClient = null;
        final Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaConfig.getRequestTimeoutMsConfig());
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "checkTopicPartitionExists");

        adminClient = AdminClient.create(config);

        try {

            // Find topic...
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topic));
            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();

            if (!stringTopicDescriptionMap.containsKey(topic)) {
                LOGGER.error(String.format("Topic doesn't exist [topic=%s]", topic));
                return false;
            }

            LOGGER.info("Found topic.");

            // Find partition...
            TopicDescription topicDescription = stringTopicDescriptionMap.get(topic);
            List<TopicPartitionInfo> partitions = topicDescription.partitions();

            for (TopicPartitionInfo p : partitions) {
                if (p.partition() == partitionNum) {
                    LOGGER.info("Found partition.");
                    return true;
                }
            }

            LOGGER.error(String.format("Partition doesn't exist [topic=%s, partition=%d]", topic, partitionNum));
            return false;

        } catch (Exception e) {
            throw new RuntimeException("Problem verifying topic partition.", e);
        } finally {
            adminClient.close();
        }
    }
}
