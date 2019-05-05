package com.notatracer.common.messaging.trading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingMessageListener extends DefaultListener {

    private static Logger LOGGER = LoggerFactory.getLogger(LoggingMessageListener.class);

    @Override
    public void onTradeMessage(TradeMessage tradeMessage) {
        super.onTradeMessage(tradeMessage);
        LOGGER.info("Heard trade msg");

    }
}
