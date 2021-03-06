package com.notatracer.messaging.util;

import com.notatracer.common.messaging.trading.TradeMessage;
import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.abstraction.MockUnit;
import net.andreinc.mockneat.abstraction.MockUnitInt;
import net.andreinc.mockneat.unit.seq.IntSeq;
import net.andreinc.mockneat.unit.seq.LongSeq;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MessageUtil {

    public static List<String> SYMBOLS = Arrays.asList("AAPL", "BUD", "CAT", "GILD", "IBM", "YHOO");
    static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/YYYY");
    static LocalDate maxDateInTheFuture = LocalDate.of(2025, 12, 31);
    public static long epochNanoCounter = 0;


    private static MockUnit<TradeMessage> getTradeGenerator(MockNeat mock) {
        IntSeq seqTradeSeqNum = mock.intSeq().start(100000);
        LongSeq seqId = mock.longSeq().start(1);
        MockUnitInt tradePriceDollarRange = mock.ints().range(0, 99999);
        MockUnitInt strikePriceDollarRange = mock.ints().range(0, 99999);
        MockUnitInt centsRange = mock.ints().range(0, 99);

        /*
                String und = underlyings.get(rand.nextInt(underlyings.size()));
        System.arraycopy(und.getBytes(), 0, undMessage.undName, 0, und.length());

         */

        return mock.filler(TradeMessage::new)
                .setter(TradeMessage::setId, seqId)
                .setter(TradeMessage::setTradeSequenceNumber, seqTradeSeqNum)
                .setter(TradeMessage::setQuantity, mock.ints().range(1, 5000))
                .setter(TradeMessage::setTradePrice, mock.fmt("#{dollars}.#{cents}").param("dollars", tradePriceDollarRange).param("cents", centsRange))
                .setter(TradeMessage::setSymbol, mock.fromStrings(SYMBOLS))
                .setter(TradeMessage::setStrikePrice, mock.fmt("#{dollars}.#{cents}").param("dollars", strikePriceDollarRange).param("cents", centsRange))
                .setter(TradeMessage::setExpirationDate, mock.localDates()
                        .future(maxDateInTheFuture)
                        .mapToString((d) -> d.format(dtf)))
                .setter(TradeMessage::setCallPut, mock.chars().from("CP"))
                .setter(TradeMessage::setBuyAccount, mock.ints().range(1, 999).mapToString())
                .setter(TradeMessage::setSellAccount, mock.ints().range(1, 999).mapToString());
    }

    public static TradeMessage tradeMessage() {
        MockNeat mock = MockNeat.threadLocal();
        return MessageUtil.getTradeGenerator(mock).get();
    }

    public static TradeMessage tradeMessage(MockNeat mock) {
        return MessageUtil.getTradeGenerator(mock).get();
    }

    public static List<TradeMessage> nTradeMessages(int n) {
        MockNeat mock = MockNeat.threadLocal();
        MockUnit<TradeMessage> tradeGenerator = getTradeGenerator(mock);
        return IntStream.range(1, n)
                .mapToObj(x -> tradeGenerator.get())
                .map(x -> {
                    x.setEpochNanos(getEpochNanos());
                    return x;
                })
                .collect(
                        Collectors.toList()
                );
    }

    private static long getEpochNanos() {
        LocalDate today = LocalDate.now();
        int NANOS_IN_SECOND = 1_000_000_000;

        ZonedDateTime tradingDateAtSOD = null;

        ZoneId ET = ZoneId.of("America/New_York");
        tradingDateAtSOD = ZonedDateTime.of(today, LocalTime.parse("09:30:00"), ET);
        long epochNanos930ET = tradingDateAtSOD.toEpochSecond() * 1000000000;

//        long sosEpochNanos = LocalDateTime.of(today, LocalTime.MIDNIGHT).withHour(9).withMinute(30).toEpochSecond(ZoneOffset.UTC) * NANOS_IN_SECOND;

        return epochNanos930ET + (epochNanoCounter++ * NANOS_IN_SECOND);
    }

    public static void main(String[] args) {
//        MockNeat mock = MockNeat.threadLocal();
//        MessageUtil.nTradeMessages(10).stream().forEach(
//                t -> System.out.println(t)
//        );
//

        LocalDate today = LocalDate.now();
        int NANOS_IN_SECOND = 1_000_000_000;

        ZonedDateTime tradingDateAtSOD = null;

        ZoneId ET = ZoneId.of("America/New_York");
        tradingDateAtSOD = ZonedDateTime.of(today, LocalTime.parse("09:30:00"), ET);
        long epochNanos930ET = tradingDateAtSOD.toEpochSecond() * 1000000000;

        long sosEpochNanos = LocalDateTime.of(today, LocalTime.MIDNIGHT).withHour(9).withMinute(30).toEpochSecond(ZoneOffset.UTC) * NANOS_IN_SECOND;

        System.out.println(String.format("1=%s, 2=%s", epochNanos930ET, sosEpochNanos));

    }
}
