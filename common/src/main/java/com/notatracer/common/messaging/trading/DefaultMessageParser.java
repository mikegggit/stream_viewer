package com.notatracer.common.messaging.trading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

/**
 * Parses fully framed raw opening messages.
 */
@Component
public class DefaultMessageParser implements MessageParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageParser.class);

    private static TradeMessage tradeMessage = new TradeMessage();

    @Override
    public void parse(ByteBuffer buf, Listener l) {
        LOGGER.debug(String.format("parse [position=%s, limit=%s, remaining=%s]", buf.position(), buf.limit(), buf.remaining()));
        byte msgType = (byte) buf.get();
        LOGGER.debug(String.format("parse [msgType=%s]", (char)msgType));

        switch(msgType) {
            case (byte)'T':
                tradeMessage.clear();
                tradeMessage.setBuf(buf);
                tradeMessage.parse(buf);
                l.onTradeMessage(tradeMessage);
                break;
            default:
                l.onUnknownMessage();
        }
    }
}
