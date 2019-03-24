package com.notatracer.common.messaging.trading;

/**
 */
public interface Listener {

    public void onTradeMessage(TradeMessage tradeMessage);

    public void onUnknownMessage();

}
