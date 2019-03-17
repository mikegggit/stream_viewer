package com.notatracer.common.messaging;

/**
 *
 */
public class DefaultListener implements Listener {

    @Override
    public void onTradeMessage(TradeMessage tradeMessage) {
        System.out.println(tradeMessage);
    }
}
