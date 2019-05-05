package com.notatracer.viewer;

import com.notatracer.common.messaging.Message;
import com.notatracer.common.messaging.trading.TradeMessage;
import com.notatracer.viewer.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.notatracer.kafka.util.KafkaUtil.topicPartitionExists;

/**
 * Simple topic viewer application capable of starting at user-supplied
 * start time.
 */
@SpringBootApplication
public class StreamViewerApp implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamViewerApp.class);
    /**
     * first message to read in format HH:mm:dd
     */
    private static final String ARG_START_TIME = "start-time";

    private static final String TIME_OPENING = "09:30:00";
    private static final String TIME_CLOSING = "16:00:00";

    private ZonedDateTime tradingDateAtSOD = null;

    private ZonedDateTime tradingDateAtEOD = null;

    private long epochNanos930ET = -1l;
    private long epochNanos400ET = -1l;

    int numPartitions = -1;

    long partitionRange = -1l;

    Message message = new TradeMessage();


    @Autowired
    private KafkaConfig kafkaConfig;

    public static void main(String[] args) {
        LOGGER.trace("StreamViewerApp::main");
        SpringApplication.run(StreamViewerApp.class, args);
    }

    @PostConstruct
    public void init() {
        LocalDate tradeDate = LocalDate.of(2019, 5, 1);
        ZoneId ET = ZoneId.of("America/New_York");
        tradingDateAtSOD = ZonedDateTime.of(tradeDate, LocalTime.parse(TIME_OPENING), ET);
        tradingDateAtEOD = ZonedDateTime.of(tradeDate, LocalTime.parse(TIME_CLOSING), ET);

        epochNanos930ET = tradingDateAtSOD.toEpochSecond() * 1000000000;
        epochNanos400ET = tradingDateAtEOD.toEpochSecond() * 1000000000;

        numPartitions = kafkaConfig.getNumPartitions();

        partitionRange = (epochNanos400ET - epochNanos930ET) / numPartitions;

        LOGGER.info("Initializing listener [epochNanos930ET={}, epochNanos400ET={}, numPartitions={}, partitionRange={}]", epochNanos930ET, epochNanos400ET, numPartitions, partitionRange);
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

    }

    private void stream(String argStartTime) {

        LocalTime startTime = LocalTime.parse(Optional.ofNullable(argStartTime).orElse(TIME_OPENING));
        LocalDateTime localDateTime = startTime.atDate(LocalDate.parse("2019-05-01"));
        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.of("America/New_York"));
        long startTimeInEpochNanos = zonedDateTime.toEpochSecond() * 1000000000;

        int partitionNum = calculatePartition(startTimeInEpochNanos);

        LOGGER.info("Streaming session [startTime={}, topic={}, initial-partition={}]", localDateTime, kafkaConfig.getTopic(), partitionNum);

        streamStartingAt(partitionNum, startTimeInEpochNanos);
    }

    private void streamStartingAt(int partitionNum, long startTimeInEpochNanos) {

        String topic = kafkaConfig.getTopic();
        assertTopicPartitionExists(topic, partitionNum, kafkaConfig);

        KafkaConsumer<String, byte[]> consumer = null;
        long startOffset;
        TopicPartition topicPartition = new TopicPartition(topic, partitionNum);

        try {
            consumer = getSingleRecordPollingConsumer();
            consumer.assign(Arrays.asList(topicPartition));

            // ensure partition contains startTime...
            assertTopicPartitionContainsStartTime(startTimeInEpochNanos, consumer, topicPartition);

            LOGGER.info(String.format("Searching for start offset [startTimeInEpochNanos=%d, partitionNum=%d]", startTimeInEpochNanos, partitionNum));

            // use binary search to get determine offset to start polling from...
            Map<TopicPartition, Long> offsetMap = consumer.endOffsets(Arrays.asList(topicPartition));
            long maxOffsetInPartition = offsetMap.get(topicPartition);
            startOffset = getInitialOffset(consumer, topicPartition, 0l, maxOffsetInPartition, startTimeInEpochNanos);

            LOGGER.info(String.format("Found offset [value=%d]", startOffset));
        } finally {
            consumer.close();
        }

    }

    private void assertTopicPartitionContainsStartTime(long startTimeInEpochNanos, KafkaConsumer<String, byte[]> consumer, TopicPartition topicPartition) {
        LOGGER.debug(String.format("assertTopicPartitionContainsStartTime [startTimeInEpochNanos=%d]", startTimeInEpochNanos));

        consumer.seek(topicPartition, 0);
        ConsumerRecords<String, byte[]> record = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

        assertOneRecord(record);

        ByteBuffer buf = ByteBuffer.allocate(1000);

        byte[] recordBytes = record.records(topicPartition).get(0).value();
        buf.put(recordBytes);
        buf.flip();

        long earliestInPartition = Message.parseEpochNanos(buf);
        System.out.println("earliest: " + earliestInPartition);
        System.out.println("msgType: " + Message.parseMsgType(buf));

        consumer.seekToEnd(Arrays.asList(topicPartition));
        System.out.println("position: " + consumer.position(topicPartition));
        consumer.seek(topicPartition, consumer.position(topicPartition) - 1);
        record = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

        assertOneRecord(record);

        recordBytes = record.records(topicPartition).get(0).value();
        buf.clear();
        buf.put(recordBytes);
        buf.flip();

        long latestInPartition = Message.parseEpochNanos(buf);
        if (! (startTimeInEpochNanos >= earliestInPartition && startTimeInEpochNanos <= latestInPartition) ) {
            LOGGER.error(String.format("Starting time is outside the range of this partition [startTimeInEpochNanos=%d, earliest=%d, latest=%d]", startTimeInEpochNanos, earliestInPartition, latestInPartition));
            throw new RuntimeException(String.format("Starting time is outside the range of this partition [startTimeInEpochNanos=%d, earliest=%d, latest=%d]", startTimeInEpochNanos, earliestInPartition, latestInPartition));
        }
    }

    private void assertOneRecord(ConsumerRecords<String, byte[]> record) {
        if (record == null || record.count() == 0) {
            LOGGER.error("No records found!");
            throw new RuntimeException("No records found!");
        }

        if (record.count() != 1) {
            LOGGER.error(String.format("Expected 1 record, found %d", record.count()));
            throw new RuntimeException(String.format("Expected 1 record, found %d", record.count()));
        }
    }

    private KafkaConsumer<String,byte[]> getSingleRecordPollingConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "SquirrelSessionConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private void assertTopicPartitionExists(String topic, int partitionNum, KafkaConfig kafkaConfig) {
        if (!topicPartitionExists(topic, partitionNum, kafkaConfig)) {
            LOGGER.error("Failed to find topic partition.");
            throw new IllegalArgumentException(String.format("Topic does not exist [name=%s]", topic));
        }
    }

    private int calculatePartition(long epochNanos) {
        LOGGER.info("calculatePartition [epochNanos={}, epochNanos930ET={}, epochNanos400ET={}]", epochNanos, epochNanos930ET, epochNanos400ET);
        long sodNanos = epochNanos - epochNanos930ET;
        int partitionNum = -1;

        if (sodNanos < partitionRange) {
            partitionNum = 0;
        } else if (sodNanos > (epochNanos400ET - epochNanos930ET - partitionRange)) {
            // Assign to last partition
            partitionNum = numPartitions - 1;
        } else {
            partitionNum = (int) (sodNanos / partitionRange);
        }
        return partitionNum;
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
//            consumer = getSingleRecordPollingConsumer();
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
    private long getInitialOffset(KafkaConsumer consumer, TopicPartition topicPartition, long min, long max, long targetVal) {

        LOGGER.info(String.format("getInitialOffset [min=%d, max=%d]", min, max));
        long nextOffsetToSearch = (min + max) / 2;

        LOGGER.info(String.format("Searching... [nextOffset=%d, maxOffset=%d]", nextOffsetToSearch, max));
        consumer.seek(topicPartition, nextOffsetToSearch);
        ConsumerRecords<String, byte[]> record = consumer.poll(100);

        assertOneRecord(record);

        // Get epoch nanos from search record and compare to desired start ts...
        ByteArrayBuffer buf = (ByteArrayBuffer) (ByteArrayBuffer.getFactory().createBuffer(10000));
        byte[] inetBytes = record.records(topicPartition).get(0).value();
        buf.put(inetBytes);
        buf.flip();

        seqCocoMsg.setBuffer(buf);

        long epochNanos = seqCocoMsg.parseTimestamp();
        byte msgType = SeqCocoMsg.crackMsgType(buf);

        LOGGER.info(String.format("search result [target=%d, found=%d]", targetVal, epochNanos));
        if (Math.abs(targetVal - epochNanos) <= 1000000000) {
            LOGGER.info(String.format("Found match [value=%d]", epochNanos));
            // found starting point
            return nextOffsetToSearch;
        } else if (targetVal < epochNanos) {
            LOGGER.info(String.format("Offset is too high [epochNanos=%d, targetVal=%d]", epochNanos, targetVal));
            return getInitialOffset(consumer, topicPartition, min, nextOffsetToSearch, targetVal);
        } else {
            LOGGER.info(String.format("Offset is too low [epochNanos=%d, targetVal=%d]", epochNanos, targetVal));
            return getInitialOffset(consumer, topicPartition, nextOffsetToSearch, max, targetVal);
        }
    }
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
//    private KafkaConsumer<String, byte[]> getSingleRecordPollingConsumer() {
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
