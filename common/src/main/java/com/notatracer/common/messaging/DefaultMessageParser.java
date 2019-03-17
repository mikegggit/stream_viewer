package com.notatracer.common.messaging;

import java.nio.ByteBuffer;

/**
 * Parses fully framed raw opening messages.
 */
public class DefaultMessageParser implements MessageParser {

    private static TradeMessage tradeMessage = new TradeMessage();

    @Override
    public void parse(ByteBuffer buf, Listener list) {
        DefaultListener l = (DefaultListener) list;
        byte msgType = (byte) buf.get();

        switch(msgType) {
            case (byte)'T':
                System.out.println("Heard TradeMessage");
                tradeMessage.clear();
                tradeMessage.setBuf(buf);
                l.onTradeMessage(tradeMessage);

                break;
            default:
                System.out.println("Unknown message type: " + (char)msgType);
                break;
        }
    }
}
