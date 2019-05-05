package com.notatracer.common.messaging;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 *
 */
public abstract class Message {

    public static final byte EMPTY_BYTE = '?';
    public static final short EMPTY_NUM = -1;
    public static final byte SPACE = (byte)' ';

    protected byte messageType = EMPTY_BYTE;
    public long id = EMPTY_NUM;
    public long epochNanos = EMPTY_NUM;

    protected ByteBuffer buf;


    public enum Lengths {
        UND(5), PRICE(8), DATE(10), ACCOUNT(3);

        int size = EMPTY_NUM;

        Lengths(int size) {
            this.size = size;
        }

        public int getSize() {
            return size;
        }
    }

    public enum ExchangeCodes {

        NYSE((byte)'N'), Nasdaq((byte)'Q');

        byte code = EMPTY_BYTE;

        ExchangeCodes(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }
    }
    public ByteBuffer getBuf() {
        return buf;
    }

    /**
     * Serializes fields into buf.
     */
    public abstract void encode(ByteBuffer buf);

    /**
     * Clears field values.
     */
    public abstract void clear();

    /**
     * Writes to a file.
     */
    public abstract void write();

    /**
     * Parses values out of previously set buf.
     */
    public abstract void parse();

    /**
     * Parses values out of buf into Message fields.
     */
    public abstract void parse(ByteBuffer buf);

    public void setBuf(ByteBuffer buf) {
        this.buf = buf;
    }

    public byte getMessageType() {
        return messageType;
    }

    public long parseId() {
        return Optional.ofNullable(buf).map(b -> b.getLong(1)).orElseThrow(() -> new IllegalStateException("Buffer not set."));
    }

    public long parseEpochNanos() {
        return Optional.ofNullable(buf).map(b -> b.getLong(9)).orElseThrow(() -> new IllegalStateException("Buffer not set."));
    }

    public static char parseMsgType(ByteBuffer buf) {
        return Optional.of(buf).get().getChar(0);
    }

    public static long parseId(ByteBuffer buf) {
        return Optional.of(buf).get().getLong(1);
    }

    public static long parseEpochNanos(ByteBuffer buf) {
        return Optional.of(buf).get().getLong(9);
    }

}
