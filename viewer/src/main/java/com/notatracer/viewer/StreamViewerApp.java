package com.notatracer.viewer;

import com.notatracer.TimeUtil;
import com.notatracer.common.messaging.Message;
import com.notatracer.common.messaging.trading.LoggingMessageListener;
import com.notatracer.common.messaging.trading.MessageParser;
import com.notatracer.common.messaging.trading.TradeMessage;
import com.notatracer.viewer.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
@SpringBootApplication(scanBasePackages = {"com.notatracer"})
public class StreamViewerApp implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamViewerApp.class);
    /**
     * first message to read in format HH:mm:dd
     */
    private static final String ARG_START_TIME = "start-time";

    private static final String TIME_OPENING = "09:30:00";
    private static final String TIME_CLOSING = "16:00:00";

    private long epochNanos930ET = -1l;
    private long epochNanos400ET = -1l;

    int numPartitions = -1;
    long partitionRange = -1l;

    Message message = new TradeMessage();

    @Autowired
    private MessageParser parser;

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

        epochNanos930ET = TimeUtil.getEpochNanos( tradeDate, LocalTime.parse(TIME_OPENING), ET);
        epochNanos400ET = TimeUtil.getEpochNanos( tradeDate, LocalTime.parse(TIME_CLOSING), ET);

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
            startFromBeginning = false;
            argStartTime = args.getOptionValues(ARG_START_TIME).get(0);
        }

        LOGGER.info("Processed arguments [start-time={}]", startFromBeginning ? "beginning" : argStartTime);

        stream(argStartTime);
    }

    private void stream(String argStartTime) {

        LocalTime startTime = LocalTime.parse(Optional.ofNullable(argStartTime).orElse(TIME_OPENING));
        long startTimeInEpochNanos = TimeUtil.getEpochNanos( LocalDate.of(2019, 5, 1), startTime, ZoneId.of("America/New_York"));

        int partitionNum = calculatePartition(startTimeInEpochNanos);

        LOGGER.info("Streaming session [startTime={}, topic={}, initial-partition={}]", startTime, kafkaConfig.getTopic(), partitionNum);

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

        try {
            // Create polling consumer
            consumer = getPollingConsumer();
            consumer.assign(Arrays.asList(topicPartition));

            pollForMessages(consumer, parser);
        } finally {
            LOGGER.info("Closing consumer.");
            consumer.close();
        }

    }

    private void pollForMessages(KafkaConsumer<String, byte[]> consumer, MessageParser parser) {

        ByteBuffer buf = ByteBuffer.allocate(10000);
        LoggingMessageListener l = new LoggingMessageListener();

        while (true) {
            // start polling...
            LOGGER.info("pollForMessages...polling...");
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            LOGGER.info("...pulled {} messages.", records.count());
            for (ConsumerRecord<String, byte[]> record : records) {
                LOGGER.debug("Processing record [offset={}]", record.offset());

                boolean roomLeftInBuf = buf.remaining() >= record.value().length;

                // if no room left, compact the buffer
                if (!roomLeftInBuf) {
                    buf.compact();
                }

                // load buf...
                buf.put(record.value());
                buf.flip();

                // parse message
                parser.parse(buf, l);

            }
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

        consumer.seekToEnd(Arrays.asList(topicPartition));
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
        setCommonConsumerProperties(props);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "StartPointScanner1");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        return new KafkaConsumer<>(props);
    }

    private KafkaConsumer<String, byte[]> getPollingConsumer() {
        Properties props = new Properties();
        setCommonConsumerProperties(props);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "SquirrelSessionConsumer1");
        return new KafkaConsumer<>(props);
    }

    private void setCommonConsumerProperties(Properties props) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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

    private long getInitialOffset(KafkaConsumer singleRecordPollingConsumer, TopicPartition topicPartition, long min, long max, long targetVal) {

        LOGGER.info("getInitialOffset [min={}, max={}]", min, max);
        long nextOffsetToSearch = (min + max) / 2;

        LOGGER.info("Searching... [nextOffset={}, maxOffset={}]", nextOffsetToSearch, max);
        singleRecordPollingConsumer.seek(topicPartition, nextOffsetToSearch);
        ConsumerRecords<String, byte[]> record = singleRecordPollingConsumer.poll(Duration.of(1, ChronoUnit.SECONDS));

        assertOneRecord(record);

        // Get epoch nanos from search record and compare to desired start ts...
        ByteBuffer buf = ByteBuffer.allocate(10000);

        byte[] recordBytes = record.records(topicPartition).get(0).value();
        buf.put(recordBytes);
        buf.flip();

        long earliestInPartition = Message.parseEpochNanos(buf);
        System.out.println("earliest: " + earliestInPartition);
        System.out.println("msgType: " + Message.parseMsgType(buf));

        long epochNanos = Message.parseEpochNanos(buf);
        char msgType = Message.parseMsgType(buf);

        LOGGER.info("Search result [target={}, found={}]", targetVal, epochNanos);
        if (Math.abs(targetVal - epochNanos) <= 1000000000) {
            LOGGER.info("Found match [value={}]", epochNanos);
            // found starting point
            return nextOffsetToSearch;
        } else if (targetVal < epochNanos) {
            LOGGER.info("Offset is too high [epochNanos={}, targetVal={}]", epochNanos, targetVal);
            return getInitialOffset(singleRecordPollingConsumer, topicPartition, min, nextOffsetToSearch, targetVal);
        } else {
            LOGGER.info("Offset is too low [epochNanos={}, targetVal={}]", epochNanos, targetVal);
            return getInitialOffset(singleRecordPollingConsumer, topicPartition, nextOffsetToSearch, max, targetVal);
        }
    }
}
