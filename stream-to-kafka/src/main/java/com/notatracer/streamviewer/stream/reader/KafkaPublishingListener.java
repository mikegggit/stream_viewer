package com.notatracer.streamviewer.stream.reader;

import com.notatracer.common.messaging.trading.DefaultListener;
import com.notatracer.common.messaging.trading.TradeMessage;
import com.notatracer.streamviewer.config.IngestConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

@Component
public class KafkaPublishingListener extends DefaultListener implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublishingListener.class);

    @Autowired
    private KafkaProducer<String, byte[]> kafkaProducer;

    @Autowired
    private IngestConfig ingestConfig;

    @Override
    public void onTradeMessage(TradeMessage tradeMessage) {
//        super.onTradeMessage(tradeMessage);
        LOGGER.info(String.format("onTradeMessage [trade=%s]", tradeMessage));

        LOGGER.info(String.format("onTradeMessage [limit=%s, position=%s]", tradeMessage.getBuf().limit(), tradeMessage.getBuf().position()));

        // write to kafka

        ByteBuffer buf = tradeMessage.getBuf();
        int len = buf.position();
        byte[] bytes = new byte[len];
        buf.position(0);
        buf.get(bytes);
        System.out.println(new String(bytes));


        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(ingestConfig.getKafka().getTopic(), bytes);
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


        /*
                LOGGER.info(String.format("buf [type=%s]", buf.getClass().getCanonicalName()));
        byte msgType = buf.get( buf.position() + 8);
        LOGGER.info(String.format("readNextInternal [position=%d, limit=%d, msgType=%s]", buf.position(), buf.limit(), (char)msgType));

        byte[] bytes = new byte[buf.limit() - buf.position()];
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(properties.getKafka().getProducer().getTopic(), "foo", bytes);
        kafkaProducer.send(producerRecord,  new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if(e != null) {
                    LOGGER.error("Problem publishing message to topic.", e);
                    e.printStackTrace();
                } else {
                    LOGGER.info("The offset of the record we just sent is: " + metadata.offset());
                }
            }
        });*/
    }

    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }
}
