package com.notatracer.streamviewer;

import com.notatracer.common.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.GZIPInputStream;

@SpringBootApplication
public class StreamIngestApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamIngestApp.class);

    public static void main(String[] args) {
        LOGGER.trace("StreamIngestApp::main");
        SpringApplication.run(StreamIngestApp.class, args);

        String path = "/tmp/session.gz";
        TradeMessage tradeMessage = new TradeMessage();

        ByteBuffer header = ByteBuffer.allocate(4);
        ByteBuffer bb = ByteBuffer.allocate(1000);

        tradeMessage.clear();

        bb.clear();

        try (GZIPInputStream zis = new GZIPInputStream(new FileInputStream(new File(path)))) {
            try (ReadableByteChannel in = Channels.newChannel(zis)) {

                MessageParser parser = new DefaultMessageParser();
                Listener l = new DefaultListener();

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

                        if (len > 0 && bb.remaining() >= len) {
                            byte msgType = (byte) bb.get();
                            tradeMessage.parse(bb);
                            LOGGER.info(tradeMessage.toString());
                        } else {

                            // get more data from stream
                            bb.compact();
                            int position = bb.position();
                            bytesRead = in.read(bb);
                            bb.flip();
                            byte msgType = (byte) bb.get();
                            tradeMessage.parse(bb);
                            LOGGER.info(tradeMessage.toString());
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
        }
    }
}
