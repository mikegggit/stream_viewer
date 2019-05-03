package com.notatracer.streamtokafka;

import com.notatracer.common.messaging.trading.MessageParser;
import com.notatracer.common.messaging.trading.TradeMessage;
import com.notatracer.streamtokafka.config.IngestConfig;
import com.notatracer.streamtokafka.stream.reader.KafkaPublishingListener;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@SpringBootApplication(scanBasePackages = {"com.notatracer.streamviewer", "com.notatracer.common.messaging"})
@EnableConfigurationProperties(IngestConfig.class)
public class StreamIngestApp implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamIngestApp.class);

    @Autowired
    private IngestConfig ingestConfig;

    @Autowired
    private KafkaPublishingListener listener;

    @Autowired
    private MessageParser parser;

    public static void main(String[] args) {
        LOGGER.trace("StreamIngestApp::main");
        SpringApplication.run(StreamIngestApp.class, args);
    }

    private void run() {
        if (true) {
            initializeTopic();
        }

//        KafkaProducer<String, byte[]> kafkaProducer = createKafkaProducer();

        String path = ingestConfig.getSessionPath();;
        TradeMessage tradeMessage = new TradeMessage();

        ByteBuffer header = ByteBuffer.allocate(4);
        ByteBuffer bb = ByteBuffer.allocate(1000);

        tradeMessage.clear();

        bb.clear();

        try (GZIPInputStream zis = new GZIPInputStream(new FileInputStream(new File(path)))) {
            try (ReadableByteChannel in = Channels.newChannel(zis)) {

//                MessageParser parser = new DefaultMessageParser();
//                Listener l = new DefaultListener();

                int bytesRead;

                // reset position to 0, limit to amount read
                while (true) {

                    bytesRead = in.read(bb);
                    if (bytesRead <= 0) {
                        return;
                    }
                    bb.flip();
                    while (bb.remaining() >= 4) {
                        int len = bb.getInt();
                        LOGGER.info(String.format("length=%s", len));

                        if (len > 0 && bb.remaining() >= len) {
                            LOGGER.info(String.format("StreamingIngestApp [position=%s, limit=%s, remaining=%s]", bb.position(), bb.limit(), bb.remaining()));
                            parser.parse(bb.slice(), listener);
                            bb.position(bb.position() + len);
                        } else {
                            // get more data from stream
                            LOGGER.info("Read more data from stream...");
                            bb.compact();
                            bytesRead = in.read(bb);
                            bb.flip();
                            parser.parse(bb.slice(), listener);
                            bb.position(bb.position() + len);
                        }
                    }
                    bb.compact();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                listener.close();
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.warn("Problem closing listener");
            }
        }
    }


    private void initializeTopic() {
        LOGGER.info("Cleaning up...");

        AdminClient adminClient = null;
        final Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ingestConfig.getBootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, ingestConfig.getRequestTimeoutMsConfig());
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "squirrelSetup");

        adminClient = KafkaAdminClient.create(config);

        String topicName = ingestConfig.getKafkaConfig().getTopic();
        try {

            // Recreate topic...

            // First delete existing...
            LOGGER.info("deleting topic...");
            try {
                DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicName));
                deleteTopicsResult.all().get();
            } catch (Exception e) {
                LOGGER.warn(String.format("Problem deleting topic [name=%s]", topicName), e);
            }

            Thread.sleep(2000l);
            // ...then create new one...
            LOGGER.info("recreating topic...");
//            NewTopic newTopic = createTopicSinglePartitionNoReplication(topicName);
            NewTopic newTopic = createTopicPartitioned(topicName, ingestConfig.getKafkaConfig().getNumPartitions());

            CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic), new CreateTopicsOptions().timeoutMs(5000));
            ((KafkaFuture)createTopicsResult.values().get(topicName)).get();

        } catch (Exception e) {
            LOGGER.error(String.format("Problem recreating topic [topic=%s]", topicName), e);
            throw new RuntimeException(String.format("Problem recreating topic [topic=%s]", topicName), e);
        } finally {
            adminClient.close();
        }

        LOGGER.info(String.format("Created topic [name=%s]", topicName));
    }

    private NewTopic createTopicPartitioned(String topicName, int numPartitions) {
        return new NewTopic(topicName, numPartitions, (short)1);
    }

    @Override
    public void run(String... args) throws Exception {
        this.run();
    }
}
