package com.notatracer.common.messaging.trading;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;

public class TradeMessageTest {

    @Test
    void encodeDecode() {
        TradeMessage trade = new TradeMessage();
        trade.id = 1l;
        trade.tradeSequenceNumber = 123456;
        trade.quantity = 500;
        trade.setTradePrice("1.51");
        trade.setSymbol("AAPL");
        trade.setStrikePrice("400.25");
        trade.setExpirationDate("05/15/2025");
        trade.setCallPut((byte) 'C');
        trade.setBuyAccount("123");
        trade.setSellAccount("456");

        ByteBuffer buf = ByteBuffer.allocate(200);

        trade.encode(buf);
        buf.flip();
        trade = new TradeMessage();

        trade.parse(buf);
        Assertions.assertEquals(1l, trade.id);

    }

    @Test
    void testEpoch() {

        LocalDateTime now = LocalDateTime.now();
        System.out.println(now.toEpochSecond(ZoneOffset.UTC));

        LocalDate today = LocalDate.now();

        now = LocalDateTime.of(today, LocalTime.MIDNIGHT);

        System.out.println(now.toEpochSecond(ZoneOffset.UTC));

        long epochNanosAtStartOfSession = now.withHour(9).withMinute(30).toEpochSecond(ZoneOffset.UTC);

        System.out.println(epochNanosAtStartOfSession);



//        System.out.println(LocalDate.now().
    }

}
