package com.aliyun.odps.tunnel.hasher;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.data.Varchar;

/**
 * Created by zhenhong.gzh on 2023/6/28.
 */

class DefaultHashFactory implements HasherFactory {

  /**
   * all type hash
   */
  private static Map<OdpsType, OdpsHasher> factoryMap = new HashMap<>();

  static {
    factoryMap.put(OdpsType.TINYINT, new TinyIntHasher());
    factoryMap.put(OdpsType.SMALLINT, new SmallIntHasher());
    factoryMap.put(OdpsType.INT, new IntHasher());
    factoryMap.put(OdpsType.BIGINT, new BigintHasher());
    factoryMap.put(OdpsType.DATETIME, new DateTimeHasher());
    factoryMap.put(OdpsType.DATE, new DateHasher());
    factoryMap.put(OdpsType.FLOAT, new FloatHasher());
    factoryMap.put(OdpsType.DOUBLE, new DoubleHasher());
    factoryMap.put(OdpsType.BOOLEAN, new BooleanHasher());
    factoryMap.put(OdpsType.CHAR, new CharHasher());
    factoryMap.put(OdpsType.VARCHAR, new VarcharHasher());
    factoryMap.put(OdpsType.STRING, new StringHasher());
    factoryMap.put(OdpsType.BINARY, new BinaryHasher());
    factoryMap.put(OdpsType.TIMESTAMP, new TimestampHasher());
    factoryMap.put(OdpsType.INTERVAL_DAY_TIME, new IntervalDayTimeHasher());
  }


  @Override
  public OdpsHasher getHasher(OdpsType type) {
    return factoryMap.get(type);
  }

  public String getName() {
    return "default";
  }

  /*
   * basic hash function for long
   */
  static int basicLongHasher(long val) {
    long l = val;
    l = (~l) + (l << 18);
    l ^= (l >> 31);
    l *= 21;
    l ^= (l >> 11);
    l += (l << 6);
    l ^= (l >> 22);
    return (int) (l);
  }

  /**
   * tinyint type hash
   */
  private static class TinyIntHasher implements OdpsHasher<Byte> {

    @Override
    public int hash(Byte val) {
      if (val == null) {
        return 0;
      }
      return basicLongHasher(val.longValue());
    }
  }


  /**
   * smallint type hash
   */
  private static class SmallIntHasher implements OdpsHasher<Short> {

    @Override
    public int hash(Short val) {
      if (val == null) {
        return 0;
      }
      return basicLongHasher(val.longValue());
    }
  }


  /**
   * Integer type hash
   */
  private static class IntHasher implements OdpsHasher<Integer> {

    @Override
    public int hash(Integer val) {
      if (val == null) {
        return 0;
      }
      return basicLongHasher(val.longValue());
    }
  }

  /**
   * Bigint type hash
   */
  private static class BigintHasher implements OdpsHasher<Long> {

    @Override
    public int hash(Long val) {
      if (val == null) {
        return 0;
      }
      return basicLongHasher(val.longValue());
    }
  }

  /**
   * Float type hash
   */
  private static class FloatHasher implements OdpsHasher<Float> {

    @Override
    public int hash(Float val) {
      if (val == null) {
        return 0;
      }
      return basicLongHasher((long) Float.floatToIntBits(val));
    }
  }

  /**
   * Double type hash
   */
  private static class DoubleHasher implements OdpsHasher<Double> {

    @Override
    public int hash(Double val) {
      if (val == null) {
        return 0;
      }
      return basicLongHasher(Double.doubleToLongBits(val));
    }
  }

  /**
   * Boolean type hash
   */
  private static class BooleanHasher implements OdpsHasher<Boolean> {

    @Override
    public int hash(Boolean val) {
      if (val == null) {
        return 0;
      }
      //it's magic number
      if (val) {
        return 0x172ba9c7;
      } else {
        return -0x3a59cb12;
      }
    }
  }

  /**
   * String type hash
   */
  private static class StringHasher implements OdpsHasher<String> {

    private static final Charset UTF8 = Charset.forName("UTF8");

    @Override
    public int hash(String val) {
      if (val == null) {
        return 0;
      }

      byte[] chars = val.getBytes(UTF8);
      int hashVal = 0;
      for (int i = 0; i < chars.length; ++i) {
        hashVal += chars[i];
        hashVal += (hashVal << 10);
        hashVal ^= (hashVal >> 6);
      }

      hashVal += (hashVal << 3);
      hashVal ^= (hashVal >> 11);
      hashVal += (hashVal << 15);

      return hashVal;
    }
  }

  /**
   * String type hash
   */
  private static class BinaryHasher implements OdpsHasher<Binary> {

    @Override
    public int hash(Binary val) {
      if (val == null) {
        return 0;
      }

      return factoryMap.get(OdpsType.STRING).hash(val.toString());
    }
  }


  /**
   * String type hash
   */
  private static class CharHasher implements OdpsHasher<Char> {

    @Override
    public int hash(Char val) {
      if (val == null) {
        return 0;
      }

      return factoryMap.get(OdpsType.STRING).hash(val.getValue());
    }
  }

  /**
   * String type hash
   */
  private static class VarcharHasher implements OdpsHasher<Varchar> {

    @Override
    public int hash(Varchar val) {
      if (val == null) {
        return 0;
      }

      return factoryMap.get(OdpsType.STRING).hash(val.getValue());
    }
  }

  /**
   * Date type hash
   */
  private static class DateHasher implements OdpsHasher<LocalDate> {

    @Override
    public LocalDate normalizeType(Object value) {
      if (value instanceof java.util.Date) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(((Date) value).getTime()),
                                       ZoneId.of("UTC")).toLocalDate();
      }

      return OdpsHasher.super.normalizeType(value);
    }

    @Override
    public int hash(LocalDate val) {
      if (val == null) {
        return 0;
      }

      return factoryMap.get(OdpsType.BIGINT).hash(val.atStartOfDay(ZoneOffset.UTC).toEpochSecond());
    }
  }


  private static class DateTimeHasher implements OdpsHasher<ZonedDateTime> {

    @Override
    public ZonedDateTime normalizeType(Object value) {
      if (value instanceof java.util.Date) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(((Date) value).getTime()),
                                       ZoneId.of("UTC"));
      }

      return OdpsHasher.super.normalizeType(value);
    }

    @Override
    public int hash(ZonedDateTime val) {
      if (val == null) {
        return 0;
      }
      Instant instant = val.toInstant();
      long epochMilli;
      try {
        epochMilli = instant.toEpochMilli();
      } catch (ArithmeticException e) {
        // Instant.toEpochMilli() have long overflow bug before jdk-8u112, we handle it here.
        // link: https://bugs.openjdk.org/browse/JDK-8169532
        epochMilli = toEpochMilli(instant.getEpochSecond(), instant.getNano());
      }
      return factoryMap.get(OdpsType.BIGINT).hash(epochMilli);
    }

    /**
     * The implementation of Instant.toEpochMilli() at jdk-8u112,
     * copy here to solve the problem that the user's jdk version lower than this one.
     */
    private long toEpochMilli(long seconds, int nanos) {
      if (seconds < 0 && nanos > 0) {
        long millis = Math.multiplyExact(seconds + 1, 1000);
        long adjustment = nanos / 1000_000 - 1000;
        return Math.addExact(millis, adjustment);
      } else {
        long millis = Math.multiplyExact(seconds, 1000);
        return Math.addExact(millis, nanos / 1000_000);
      }
    }
  }

  /**
   * Timestamp type hash
   */
  private static class TimestampHasher implements OdpsHasher<Instant> {

    @Override
    public Instant normalizeType(Object value) {
      if (value instanceof java.sql.Timestamp) {
        return ((Timestamp) value).toInstant();
      }

      return OdpsHasher.super.normalizeType(value);
    }

    @Override
    public int hash(Instant val) {
      if (val == null) {
        return 0;
      }
      long seconds = val.getEpochSecond();
      int nanos = val.getNano();
      seconds <<= 30;
      seconds |= nanos;
      return basicLongHasher(seconds);
    }
  }

  /**
   * Interval Day to time type hash
   */
  private static class IntervalDayTimeHasher implements OdpsHasher<IntervalDayTime> {

    @Override
    public int hash(IntervalDayTime val) {
      if (val == null) {
        return 0;
      }

      long totalSec = val.getTotalSeconds();
      int nanos = val.getNanos();
      totalSec <<= 30;
      totalSec |= nanos;
      return basicLongHasher(totalSec);
    }
  }
}
