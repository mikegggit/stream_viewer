package com.notatracer;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class TimeUtil {

    public static final int NANOS_PER_SECOND = 1_000_000_000;

    public static long getEpochNanos(LocalDate date, LocalTime time, ZoneId zoneId) {
        ZonedDateTime zdt = ZonedDateTime.of(date, time, zoneId);
        return zdt.toEpochSecond() * NANOS_PER_SECOND;
    }

    public static String formatEpochNanos(long epochNanos, ZoneId zoneId) {
        long epochSeconds = epochNanos / NANOS_PER_SECOND;
        int nanoOfSecond = (int)(epochNanos % NANOS_PER_SECOND);
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(epochSeconds, nanoOfSecond, zoneId.getRules().getOffset(LocalDateTime.now()));
        return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
}
