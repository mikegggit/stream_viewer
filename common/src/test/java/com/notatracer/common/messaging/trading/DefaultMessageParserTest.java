package com.notatracer.common.messaging.trading;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class DefaultMessageParserTest {

    @Test
    void parseTradeBytes() {
        DefaultMessageParser parser = new DefaultMessageParser();
        Listener l = Mockito.mock(Listener.class);

        TradeMessage trade = new TradeMessage();

        byte[] ba = new byte[200];

        ByteBuffer buf = ByteBuffer.wrap(ba);
        buf.put(trade.getMessageType());
        trade.encode(buf);

        parser.parse(buf, l);
        Mockito.verify(l).onTradeMessage(Mockito.any(TradeMessage.class));
    }

    @Test
    void parseUnknownMessage() {
        DefaultMessageParser parser = new DefaultMessageParser();
        Listener l = Mockito.mock(Listener.class);

        byte[] ba = new byte[200];

        ByteBuffer buf = ByteBuffer.wrap(ba);
        buf.put((byte)'X');
        buf.flip();
        parser.parse(buf, l);
        Mockito.verify(l).onUnknownMessage();
    }
}
