package com.notatracer.util;

import com.notatracer.common.messaging.TradeMessage;
import com.notatracer.messaging.util.MessageUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPOutputStream;

public class ZipSessionGenerator {


    private void generate(String outPath) {
        TradeMessage tradeMessage = new TradeMessage();

        ByteBuffer header = ByteBuffer.allocate(4);
        ByteBuffer bb = ByteBuffer.allocate(1000);

        tradeMessage.clear();

        File out = new File(outPath);
        boolean append = false;
//        FileOutputStream fos = new FileOutputStream(out);
        
        try (FileChannel channel = new FileOutputStream(out, append).getChannel()) {
//            GZIPOutputStream zos = new GZIPOutputStream(fos);
            MessageUtil.nTradeMessages(1000)
                    .stream()
                    .forEach(
                            message -> {
                                tradeMessage.clear();
                                bb.clear();
                                header.clear();
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
        ZipSessionGenerator generator = new ZipSessionGenerator();
        generator.generate("/tmp/session.gz");


    }
}
