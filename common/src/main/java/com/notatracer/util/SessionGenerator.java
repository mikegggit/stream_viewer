package com.notatracer.util;

import com.notatracer.common.messaging.trading.TradeMessage;
import com.notatracer.messaging.util.MessageUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class SessionGenerator {
    private void generate(String outPath) {
        TradeMessage tradeMessage = new TradeMessage();

        ByteBuffer header = ByteBuffer.allocate(4);
        ByteBuffer bb = ByteBuffer.allocate(1000);

        tradeMessage.clear();

        File out = new File(outPath);
        boolean append = false;

        AtomicInteger counter = new AtomicInteger(1);
        try (FileChannel channel = new FileOutputStream(out, append).getChannel()) {
            MessageUtil.nTradeMessages(100)
                    .stream()
                    .forEach(
                            message -> {
                                System.out.println(counter.getAndIncrement());
                                System.out.println(message);
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

                                    channel.write(header);
                                    channel.write(slice);

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
        SessionGenerator generator = new SessionGenerator();
        generator.generate("/tmp/session.out");
    }
}
