package com.notatracer.common.messaging.trading;

import com.notatracer.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class LoggingMessageListener extends DefaultListener {

    private static Logger LOGGER = LoggerFactory.getLogger(LoggingMessageListener.class);

    private BigDecimal tradePrice;
    private BigDecimal strikePrice;
    private StringBuilder productStringSB = new StringBuilder();

    @Override
    public void onTradeMessage(TradeMessage tradeMessage) {
        super.onTradeMessage(tradeMessage);
        StringBuffer formatString = new StringBuffer("%-20s, id=%d, time=%s, tradeSeqNum=%d, qty=%d, price=%s, product={%s}, BUY: account=%s, SELL: account=%s");

        tradePrice = new BigDecimal(new String(tradeMessage.getTradePrice()));
        strikePrice = new BigDecimal(new String(tradeMessage.getStrikePrice()));

        productStringSB.delete(0, productStringSB.length());

        productStringSB
                .append(new String(tradeMessage.getSymbol()))
                .append(" ")
                .append(tradeMessage.getCallPut() == (byte)'C' ? "(C)ALL" : "(P)UT")
                .append(" ")
                .append(strikePrice)
                .append(" ")
                .append(new String(tradeMessage.getExpirationDate()))
        ;

        System.out.println(
                String.format(
                        formatString.toString(),
                        "TradeMessage(" + (char)tradeMessage.getMessageType() + ")",
                        tradeMessage.getId(),
                        TimeUtil.formatEpochNanos(tradeMessage.getEpochNanos(), ZoneId.of("America/New_York")),
                        tradeMessage.getTradeSequenceNumber(),
                        tradeMessage.getQuantity(),
                        tradePrice,
                        productStringSB.toString(),
                        new String(tradeMessage.getBuyAccount()),
                        new String(tradeMessage.getSellAccount())));
        //        System.out.println();
//        String timeString = tradeMessage.epochNanos

    }

    public static void main(String[] args) {
        long nanos = 1556734945000000000l;
        long epochSeconds = nanos / 1_000_000_000l;
        int nanoOfSecond = (int)(nanos % 1_000_000_000);
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(epochSeconds, nanoOfSecond, ZoneId.of("America/New_York").getRules().getOffset(LocalDateTime.now()));
        System.out.println(localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }
}
