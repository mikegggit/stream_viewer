package com.notatracer.viewer;

import com.notatracer.viewer.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.*;
import java.util.*;

@SpringBootApplication
public class StreamViewerApp implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamViewerApp.class);
    /**
     * first message to read in format HH:mm:dd
     */
    private static final String ARG_START_TIME = "start-time";

    @Autowired
    private KafkaConfig kafkaConfig;

    public static void main(String[] args) {
        LOGGER.trace("StreamViewerApp::main");
        SpringApplication.run(StreamViewerApp.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        LOGGER.info("Application started w/ cmd-line args: {}", Arrays.toString(args.getSourceArgs()));

        boolean startFromBeginning = true;
        String argStartTime = null;
        LocalTime startTime = null;

        Set<String> optionNames = args.getOptionNames();

        if (args.containsOption(ARG_START_TIME)) {
            System.out.println("mike");
            startFromBeginning = false;
            argStartTime = args.getOptionValues(ARG_START_TIME).get(0);
        }

        LOGGER.info("Processed arguments [start-time={}]", startFromBeginning ? "beginning" : argStartTime);

        stream(argStartTime);
//        String startTimeString = ;
//        LocalTime.parse(startTimeString);

    }

    private void stream(String argStartTime) {

        LOGGER.info("streaming session [argStartTime={}, topic={}]", argStartTime, kafkaConfig.getTopic());
    }
//
//    public void process(String topic, int partition, String startTimeString) {
//        LOGGER.info(String.format("process [topic=%s, partition=%d]", topic, partition));
//
//        // Verify topic / partition exists...
//        if (!topicPartitionExists(topic, partition)) {
//            LOGGER.error("Failed to find topic partition.");
//            return;
//        }
//
//        // Calculate start time in epoch nanos
//        LocalDateTime localDateTime = LocalTime.parse(startTimeString).atDate(LocalDate.parse("2018-10-04"));
//        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.of("America/New_York"));
//        long startTimeInEpochNanos = zonedDateTime.toEpochSecond() * 1000000000;
//
//        LOGGER.info(String.format("Searching for start [startTimeInEpochNanos=%d]", startTimeInEpochNanos));
//
//        // Get last offset in topic...
//        // First, create consumer for the binary search
//        KafkaConsumer<String, byte[]> consumer = null;
//        long startOffset;
//        TopicPartition topicPartition = new TopicPartition(topic, partition);
//
//        try {
//            consumer = getSearchConsumer();
//            consumer.assign(Arrays.asList(topicPartition));
//
//            // ensure partition contains startTime...
//            ensurePartitionForStartTime(startTimeInEpochNanos, consumer, topicPartition);
//
//            // use binary search to get determine offset to start polling from...
//            Map<TopicPartition, Long> offsetMap = consumer.endOffsets(Arrays.asList(topicPartition));
//            long maxOffset = offsetMap.get(topicPartition);
//            startOffset = getInitialOffset(consumer, topicPartition, 0l, maxOffset, startTimeInEpochNanos);
//
//            LOGGER.info(String.format("Found offset [value=%d]", startOffset));
//        } finally {
//            consumer.close();
//        }
//
//        try {
//            // Create polling consumer
//            consumer = getPollingConsumer();
//            consumer.assign(Arrays.asList(topicPartition));
//
//            CocoSeqMessageParser parser = new CocoSeqMessageParser();
//            ByteArrayBuffer buf = (ByteArrayBuffer) (ByteArrayBuffer.getFactory().createBuffer(10000));
//
//            pollForMessages(consumer, parser, buf);
//        } finally {
//            consumer.close();
//        }
//    }
//
//    private KafkaConsumer<String, byte[]> getPollingConsumer() {
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
//        props.put(ConsumerConfig.GROUP_ID_CONFIG,
//                "SquirrelSessionConsumer");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                ByteArrayDeserializer.class);
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        return new KafkaConsumer<>(props);
//    }
//
//    private long getInitialOffset(KafkaConsumer consumer, TopicPartition topicPartition, long min, long max, long targetVal) {
//
//        LOGGER.info(String.format("getInitialOffset [min=%d, max=%d]", min, max));
//        long nextOffsetToSearch = (min + max) / 2;
//
//        LOGGER.info(String.format("Searching... [nextOffset=%d, maxOffset=%d]", nextOffsetToSearch, max));
//        consumer.seek(topicPartition, nextOffsetToSearch);
//        ConsumerRecords<String, byte[]> record = consumer.poll(100);
//
//        assertOneRecord(record);
//
//        // Get epoch nanos from search record and compare to desired start ts...
//        ByteArrayBuffer buf = (ByteArrayBuffer) (ByteArrayBuffer.getFactory().createBuffer(10000));
//        byte[] inetBytes = record.records(topicPartition).get(0).value();
//        buf.put(inetBytes);
//        buf.flip();
//
//        seqCocoMsg.setBuffer(buf);
//
//        long epochNanos = seqCocoMsg.parseTimestamp();
//        byte msgType = SeqCocoMsg.crackMsgType(buf);
//
//        LOGGER.info(String.format("search result [target=%d, found=%d]", targetVal, epochNanos));
//        if (Math.abs(targetVal - epochNanos) <= 1000000000) {
//            LOGGER.info(String.format("Found match [value=%d]", epochNanos));
//            // found starting point
//            return nextOffsetToSearch;
//        } else if (targetVal < epochNanos) {
//            LOGGER.info(String.format("Offset is too high [epochNanos=%d, targetVal=%d]", epochNanos, targetVal));
//            return getInitialOffset(consumer, topicPartition, min, nextOffsetToSearch, targetVal);
//        } else {
//            LOGGER.info(String.format("Offset is too low [epochNanos=%d, targetVal=%d]", epochNanos, targetVal));
//            return getInitialOffset(consumer, topicPartition, nextOffsetToSearch, max, targetVal);
//        }
//    }
//
//    private void ensurePartitionForStartTime(long startTimeInEpochNanos, KafkaConsumer<String, byte[]> consumer, TopicPartition topicPartition) {
//        LOGGER.info(String.format("ensurePartitionForStartTime [startTimeInEpochNanos=%d]", startTimeInEpochNanos));
//
//        consumer.seek(topicPartition, 0);
//        System.out.println("position: " + consumer.position(topicPartition));
//
//        ConsumerRecords<String, byte[]> record = consumer.poll(100);
//        assertOneRecord(record);
//
//        ByteArrayBuffer buf = (ByteArrayBuffer) (ByteArrayBuffer.getFactory().createBuffer(10000));
//        byte[] inetBytes = record.records(topicPartition).get(0).value();
//        buf.put(inetBytes);
//        buf.flip();
//
//        seqCocoMsg.setBuffer(buf);
//        seqCocoMsg.getAll();
//
//
//        long earliestInPartition = seqCocoMsg.timestamp;
//        System.out.println("earliest: " + earliestInPartition);
//        System.out.println("msgType: " + (char) seqCocoMsg.msgType);
//
//        long lastCommittedOffset = consumer.committed(topicPartition).offset();
//        consumer.seek(topicPartition, lastCommittedOffset - 1);
//
//        System.out.println("position: " + consumer.position(topicPartition));
//        record = consumer.poll(100);
//
//        assertOneRecord(record);
//
//        inetBytes = record.records(topicPartition).get(0).value();
//        buf.clear();
//        buf.put(inetBytes);
//        buf.flip();
//
//        seqCocoMsg.setBuffer(buf);
//        long latestInPartition = seqCocoMsg.parseTimestamp();
//        if (!(startTimeInEpochNanos >= earliestInPartition && startTimeInEpochNanos <= latestInPartition)) {
//            LOGGER.error(String.format("Starting time is outside the range of this partition [startTimeInEpochNanos=%d, earliest=%d, latest=%d]", startTimeInEpochNanos, earliestInPartition, latestInPartition));
//            throw new RuntimeException(String.format("Starting time is outside the range of this partition [startTimeInEpochNanos=%d, earliest=%d, latest=%d]", startTimeInEpochNanos, earliestInPartition, latestInPartition));
//        }
//    }
//
//    private void assertOneRecord(ConsumerRecords<String, byte[]> record) {
//        if (record == null || record.count() == 0) {
//            LOGGER.error("No records found!");
//            throw new RuntimeException("No records found!");
//        }
//
//        if (record.count() != 1) {
//            LOGGER.error(String.format("Expected 1 record, found %d", record.count()));
//            throw new RuntimeException(String.format("Expected 1 record, found %d", record.count()));
//        }
//    }
//
//    private KafkaConsumer<String, byte[]> getSearchConsumer() {
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
//        props.put(ConsumerConfig.GROUP_ID_CONFIG,
//                "SquirrelSessionConsumer");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
//                ByteArrayDeserializer.class);
//        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        return new KafkaConsumer<>(props);
//    }
//
//    private boolean topicPartitionExists(String topic, int partition) {
//        LOGGER.info(String.format("Check if TopicPartition exists [topic=%s, partition=%d]", topic, partition));
//
//        AdminClient adminClient = null;
//        final Map<String, Object> config = new HashMap<>();
//        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
//        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, properties.getRequestTimeoutMsConfig());
//        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "checkTopicPartitionExists");
//
//        adminClient = AdminClient.create(config);
//
//        try {
//
//            // Find topic...
//            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topic));
//            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
//
//            if (!stringTopicDescriptionMap.containsKey(topic)) {
//                LOGGER.error(String.format("Topic doesn't exist [topic=%s]", topic));
//                return false;
//            }
//
//            LOGGER.info("Found topic.");
//
//            // Find partition...
//            TopicDescription topicDescription = stringTopicDescriptionMap.get(topic);
//            List<TopicPartitionInfo> partitions = topicDescription.partitions();
//
//            for (TopicPartitionInfo p : partitions) {
//                if (p.partition() == partition) {
//                    LOGGER.info("Found partition.");
//                    return true;
//                }
//            }
//
//            LOGGER.error(String.format("Partition doesn't exist [topic=%s, partition=%d]", topic, partition));
//            return false;
//
//        } catch (Exception e) {
//            throw new RuntimeException("Problem verifying topic partition.", e);
//        } finally {
//            adminClient.close();
//        }
//    }
}
