package com.notatracer.streamtokafka.stream.reader;

import com.notatracer.common.messaging.trading.DefaultListener;
import com.notatracer.common.messaging.trading.TradeMessage;
import com.notatracer.streamtokafka.config.IngestConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Component
public class KafkaPublishingListener extends DefaultListener implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublishingListener.class);

    @Autowired
    private KafkaProducer<String, byte[]> kafkaProducer;

    @Autowired
    private IngestConfig ingestConfig;

    private ZonedDateTime tradingDateAtSOD = null;

    private ZonedDateTime tradingDateAtEOD = null;

    private long epochNanos930ET = -1l;
    private long epochNanos400ET = -1l;

    int numPartitions = -1;

    long partitionRange = -1l;

//    public KafkaPublishingListener() {}

    @PostConstruct
    public void init() {
        LocalDate tradeDate = LocalDate.now();
        ZoneId ET = ZoneId.of("America/New_York");
        tradingDateAtSOD = ZonedDateTime.of(tradeDate, LocalTime.parse("09:30:00"), ET);
        tradingDateAtEOD = ZonedDateTime.of(tradeDate, LocalTime.parse("16:00:00"), ET);

        epochNanos930ET = tradingDateAtSOD.toEpochSecond() * 1000000000;
        epochNanos400ET = tradingDateAtEOD.toEpochSecond() * 1000000000;

        numPartitions = ingestConfig.getKafkaConfig().getNumPartitions();

        partitionRange = (epochNanos400ET - epochNanos930ET) / numPartitions;

        LOGGER.info(String.format("Initializing listener [epochNanos930ET=%s, epochNanos400ET=%s, numPartitions=%s, partitionRange=%s]", epochNanos930ET, epochNanos400ET, numPartitions, partitionRange));
    }

    @Override
    public void onTradeMessage(TradeMessage tradeMessage) {

        LOGGER.debug(String.format("onTradeMessage [trade=%s, limit=%s, position=%s]", tradeMessage, tradeMessage.getBuf().limit(), tradeMessage.getBuf().position()));

        // write to kafka

        ByteBuffer buf = tradeMessage.getBuf();
        int len = buf.position();
        byte[] bytes = new byte[len];
        buf.position(0);
        buf.get(bytes);
        System.out.println(new String(bytes));

        long epochNanos = tradeMessage.getEpochNanos();
        int tgtPartition = calculatePartition(epochNanos);

        LOGGER.info(String.format("Publishing message to kafka [trade=%s, msgType=%s, epochNanos=%s, partition=%d]", tradeMessage, (char)tradeMessage.getMessageType(), epochNanos, tgtPartition));

        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(ingestConfig.getKafkaConfig().getTopic(), tgtPartition, null, bytes);

        kafkaProducer.send(producerRecord,  new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if(e != null) {
                    LOGGER.error("Problem publishing message to topic.", e);
                    e.printStackTrace();
                } else {
                    LOGGER.info(String.format("Received ack [partition=%d, offset=%d]", metadata.partition(), metadata.offset()));
                }
            }
        });
    }

    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }

    private int calculatePartition(long epochNanos) {
        // calculate partition...
        long sodNanos = epochNanos - epochNanos930ET;
        int partitionNum = -1;

        if (sodNanos < partitionRange) {
            partitionNum = 0;
        } else if (sodNanos > (epochNanos400ET - epochNanos930ET - partitionRange)) {
            // Assign to last partition
            partitionNum = numPartitions - 1;
        } else {
            partitionNum = (int)(sodNanos / partitionRange);
        }
        return partitionNum;
    }

}
