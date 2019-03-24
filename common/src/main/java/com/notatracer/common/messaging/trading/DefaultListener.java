package com.notatracer.common.messaging.trading;

/**
 *
 */
public class DefaultListener implements Listener {

    @Override
    public void onTradeMessage(TradeMessage tradeMessage) {
        System.out.println(tradeMessage);
    }

    @Override
    public void onUnknownMessage() {

    }
}
