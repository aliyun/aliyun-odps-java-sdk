package com.aliyun.odps.io;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TimestampWritableTest {

    private static final ZoneId shanghai = ZoneId.of("Asia/Shanghai");

    static class Case {
        ZonedDateTime dateTime;
        Instant instant;
        Timestamp timestamp;
        long ms;
        long sec;
        int nano;

        Case(ZonedDateTime dateTime) {
            this.dateTime = dateTime;
            this.instant = dateTime.toInstant();
            this.timestamp = Timestamp.from(instant);
            this.ms = instant.toEpochMilli();
            this.sec = instant.getEpochSecond();
            this.nano = instant.getNano();
        }

        @Override
        public String toString() {
            return timestamp + "\tms: " + ms + "\tnano: " + nano + "\tsec: " + sec;
        }
    }

    /**
     * set by timestamp / sec + nano / ms
     * get timestamp / sec + nano / ms
     * test set/get combination
     */
    @Test
    public void testSetGetByTimestamp() {

        ZonedDateTime[] dates = {
                ZonedDateTime.of(1899, 12, 31, 23, 59, 59, 999999999, shanghai),
                ZonedDateTime.of(1900, 1, 1, 0, 0, 0, 0, shanghai),
                ZonedDateTime.of(1900, 1, 1, 7, 59, 59, 999999999, shanghai),
                ZonedDateTime.of(1900, 1, 1, 8, 0, 0, 0, shanghai),
                ZonedDateTime.of(1966, 12, 31, 23, 59, 59, 999000000, shanghai),
                ZonedDateTime.of(1969, 12, 31, 23, 59, 59, 999999999, shanghai),
                ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, shanghai),
                ZonedDateTime.of(1970, 1, 1, 7, 59, 59, 999999999, shanghai),
                ZonedDateTime.of(1970, 1, 1, 8, 0, 0, 0, shanghai),
                ZonedDateTime.of(2000, 1, 1, 1, 1, 1, 1, shanghai)
        };

        List<Case> cases = Arrays.stream(dates).map(Case::new).collect(Collectors.toList());

        for (Case c: cases) {
            System.out.println(c);

            TimestampWritable writable = new TimestampWritable();
            writable.setTimestamp(c.timestamp);

            assertEquals(c.timestamp.toString(), writable.getTimestamp().toString());
            assertEquals(c.ms, writable.get());
            assertEquals(c.nano, writable.getNanos());
            assertEquals(c.sec, writable.getTotalSeconds());

            writable.set(c.sec, c.nano);
            assertEquals(c.timestamp.toString(), writable.getTimestamp().toString());
            assertEquals(c.ms, writable.get());
            assertEquals(c.nano, writable.getNanos());
            assertEquals(c.sec, writable.getTotalSeconds());

            writable.set(c.ms);
            long nanoMsPart = c.nano / 1000_000 * 1000_000L;
            c.timestamp.setNanos((int) nanoMsPart);
            assertEquals(c.timestamp.toString(), writable.getTimestamp().toString());
            assertEquals(c.ms, writable.get());
            assertEquals(nanoMsPart, writable.getNanos());
            assertEquals(c.sec, writable.getTotalSeconds());
        }
    }

}