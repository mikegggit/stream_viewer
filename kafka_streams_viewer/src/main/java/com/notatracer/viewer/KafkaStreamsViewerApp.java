package com.notatracer.viewer;

import com.notatracer.common.messaging.trading.TradeMessage;
import com.notatracer.viewer.config.KafkaConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.ByteBuffer;
import java.util.Properties;

@SpringBootApplication(scanBasePackages = {"com.notatracer"})
public class KafkaStreamsViewerApp implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsViewerApp.class);

    @Autowired
    private KafkaConfig kafkaConfig;

    public static void main(String[] args) {
        LOGGER.trace("KafkaStreamsViewerApp::main");
        SpringApplication.run(KafkaStreamsViewerApp.class, args);
    }

    private void run() {
        LOGGER.info("run");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaStreamsViewerApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();

        TradeMessage tradeMessage = new TradeMessage();
        ByteBuffer buf = ByteBuffer.allocate(10000);

        KStream<String, byte[]> source = builder.stream(kafkaConfig.getTopic());
        source.filter((k, v) -> {
            if (buf.remaining() < v.length) {
                buf.compact();
            }

            buf.put(v);
            buf.flip();
            buf.get();
            tradeMessage.parse(buf);
            return tradeMessage.quantity < 100;
    }).foreach(new ForeachAction<String, byte[]>() {
            @Override
            public void apply(String key, byte[] value) {
                if (buf.remaining() < value.length) {
                    buf.compact();
                }

                buf.put(value);
                buf.flip();
                buf.get();
                tradeMessage.parse(buf);
                System.out.println(tradeMessage);
            }
        });

        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.run();
    }
}
