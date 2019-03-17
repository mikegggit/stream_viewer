package com.notatracer.common.messaging;

import com.google.common.base.MoreObjects;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class TradeMessage extends Message {
    public TradeMessage() {
        this.messageType = 'T';
    }

    public long id = EMPTY_NUM;
    public int tradeSequenceNumber = EMPTY_NUM;
    public int quantity = EMPTY_NUM;
    public byte[] tradePrice = new byte[Lengths.PRICE.getSize()];

    // Strike
    public byte[] symbol = new byte[Lengths.UND.getSize()];
    public byte[] strikePrice = new byte[Lengths.PRICE.getSize()];
    public byte[] expirationDate = new byte[Lengths.DATE.getSize()];
    public byte callPut = EMPTY_BYTE;

    // buyside
    public byte[] buyAccount = new byte[Lengths.ACCOUNT.getSize()];

    // sellside
    public byte[] sellAccount = new byte[Lengths.ACCOUNT.getSize()];

    @Override
    public void encode(ByteBuffer buf) {
        buf.put(this.getMessageType());
        buf.putLong(this.id);
        buf.putInt(this.tradeSequenceNumber);
        buf.putInt(this.quantity);
        buf.put(this.tradePrice);

        // strike
        buf.put(this.symbol);
        buf.put(this.strikePrice);
        buf.put(this.expirationDate);
        buf.put(this.callPut);

        // buySide
        buf.put(this.buyAccount);

        // sellSide
        buf.put(this.sellAccount);
    }

    @Override
    public void clear() {
        this.id = EMPTY_NUM;
        this.tradeSequenceNumber = EMPTY_NUM;
        this.quantity = EMPTY_NUM;
        Arrays.fill(this.tradePrice, SPACE);

        // strike
        Arrays.fill(this.symbol, SPACE);
        Arrays.fill(this.strikePrice, SPACE);
        Arrays.fill(this.expirationDate, SPACE);
        this.callPut = EMPTY_BYTE;

        // buySide
        Arrays.fill(this.buyAccount, SPACE);

        // sellSide
        Arrays.fill(this.sellAccount, SPACE);

    }

    @Override
    public void write() {

    }

    @Override
    public void parse(ByteBuffer buf) {
        this.id = buf.getLong();
        this.tradeSequenceNumber = buf.getInt();
        this.quantity = buf.getInt();
        buf.get(this.tradePrice);

        // strike
        buf.get(this.symbol);
        buf.get(this.strikePrice);
        buf.get(this.expirationDate);
        this.callPut = buf.get();

        // buySide
        buf.get(this.buyAccount);

        // sellSide
        buf.get(this.sellAccount);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("tradeSequenceNumber", tradeSequenceNumber)
                .add("quantity", quantity)
                .add("tradePrice", new String(tradePrice))
                .add("symbol", new String(symbol))
                .add("strikePrice", new String(strikePrice))
                .add("expirationDate", new String(expirationDate))
                .add("callPut", (char)callPut)
                .add("buyAccount", new String(buyAccount))
                .add("sellAccount", new String(sellAccount))œ
                .toString();
    }


    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getTradeSequenceNumber() {
        return tradeSequenceNumber;
    }

    public void setTradeSequenceNumber(int tradeSequenceNumber) {
        this.tradeSequenceNumber = tradeSequenceNumber;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public byte[] getTradePrice() {
        return tradePrice;
    }

    public void setTradePrice(byte[] tradePrice) {
        this.tradePrice = tradePrice;
    }

    public void setTradePrice(String tradePrice) {
        System.arraycopy(tradePrice.getBytes(), 0, this.tradePrice, 0, tradePrice.length());
    }

    public byte[] getSymbol() {
        return symbol;
    }

    public void setSymbol(byte[] symbol) {
        this.symbol = symbol;
    }

    public void setSymbol(String symbol) {
        System.arraycopy(symbol.getBytes(), 0, this.symbol, 0, symbol.length());
    }

    public byte[] getStrikePrice() {
        return strikePrice;
    }

    public void setStrikePrice(byte[] strikePrice) {
        this.strikePrice = strikePrice;
    }

    public void setStrikePrice(String strikePrice) {
        System.arraycopy(strikePrice.getBytes(), 0, this.strikePrice, 0, strikePrice.length());
    }

    public byte[] getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(byte[] expirationDate) {
        this.expirationDate = expirationDate;
    }

    public void setExpirationDate(String expirationDate) {
        System.arraycopy(expirationDate.getBytes(), 0, this.expirationDate, 0, expirationDate.length());
    }

    public byte getCallPut() {
        return callPut;
    }

    public void setCallPut(byte callPut) {
        this.callPut = callPut;
    }

    public void setCallPut(char callPut) {
        this.callPut = (byte)callPut;
    }
    public byte[] getBuyAccount() {
        return buyAccount;
    }

    public void setBuyAccount(byte[] buyAccount) {
        this.buyAccount = buyAccount;
    }

    public void setBuyAccount(String buyAccount) {
        System.arraycopy(buyAccount.getBytes(), 0, this.buyAccount, 0, buyAccount.length());
    }

    public byte[] getSellAccount() {
        return sellAccount;
    }

    public void setSellAccount(String sellAccount) {
        System.arraycopy(sellAccount.getBytes(), 0, this.sellAccount, 0, sellAccount.length());
    }

    public void setSellAccount(byte[] sellAccount) {
        this.sellAccount = sellAccount;
    }
}
