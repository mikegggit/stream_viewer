package com.notatracer.util;

import com.notatracer.common.messaging.trading.TradeMessage;
import com.notatracer.messaging.util.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class ZipSessionGenerator {

    private static Logger LOGGER = LoggerFactory.getLogger(ZipSessionGenerator.class.getName());

    private void generate(String outPath) {
        TradeMessage tradeMessage = new TradeMessage();

        ByteBuffer header = ByteBuffer.allocate(4);
        ByteBuffer bb = ByteBuffer.allocate(1000);

        tradeMessage.clear();

        File f = new File(outPath);
        boolean append = false;

        AtomicInteger counter = new AtomicInteger(1);
        try (GZIPOutputStream zos = new GZIPOutputStream(new FileOutputStream(f, append))) {
            WritableByteChannel out = Channels.newChannel(zos);
            MessageUtil.nTradeMessages(20000)
                    .stream()
                    .forEach(
                            message -> {
//                                System.out.println(counter.getAndIncrement());
//                                System.out.println(message);
                                tradeMessage.clear();
                                bb.clear();
                                header.clear();
                                bb.put(message.getMessageType());
                                message.encode(bb);
                                bb.flip();
                                try {
                                    ByteBuffer slice = bb.slice();
                                    header.putInt(slice.remaining());
                                    header.flip();

                                    out.write(header);
                                    out.write(slice);

                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                    );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ZipSessionGenerator generator = new ZipSessionGenerator();

        final LocalDate now = LocalDate.now();
        String nowString = now.format(DateTimeFormatter.ofPattern("MM-dd-yyyy"));
        String filename = "session-" + nowString + ".gz";
        String path = "/tmp" + "/" + filename;

        LOGGER.info("Creating session file [path={}]", path);
        LOGGER.info("here");
        generator.generate(path);
    }
}
