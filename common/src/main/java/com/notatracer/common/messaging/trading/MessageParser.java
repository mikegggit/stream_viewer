package com.notatracer.common.messaging.trading;

import com.notatracer.common.messaging.trading.Listener;

import java.nio.ByteBuffer;

/**
 *
 */
public interface MessageParser {

    public abstract void parse(ByteBuffer buf, Listener l);

}
