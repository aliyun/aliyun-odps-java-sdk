package com.aliyun.odps.tunnel.hasher;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.IntervalDayTime;

/**
 * Test hash for every type
 */
public class TypeHasherTest {

  @Test
  public void testIntHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.INT, 0));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.INT, null));
    Assert.assertEquals(357654460, TypeHasher.hash(OdpsType.INT, 1));
    Assert.assertEquals(357653185, TypeHasher.hash(OdpsType.INT, -1));
    Assert.assertEquals(-718433742, TypeHasher.hash(OdpsType.INT, 10));
    Assert.assertEquals(-718434491, TypeHasher.hash(OdpsType.INT, -10));
    Assert.assertEquals(-1788925397, TypeHasher.hash(OdpsType.INT, 127));
    Assert.assertEquals(-1788925397,
                        TypeHasher.hash(OdpsType.INT, ((Byte) Byte.MAX_VALUE).intValue()));
    Assert.assertEquals(-1465040036, TypeHasher.hash(OdpsType.INT, -128));
    Assert.assertEquals(-1465040036,
                        TypeHasher.hash(OdpsType.INT, ((Byte) Byte.MIN_VALUE).intValue()));
    Assert.assertEquals(-1575889593, TypeHasher.hash(OdpsType.INT, 32767));
    Assert.assertEquals(-1575889593,
                        TypeHasher.hash(OdpsType.INT, ((Short) Short.MAX_VALUE).intValue()));
    Assert.assertEquals(-1388281606, TypeHasher.hash(OdpsType.INT, -32768));
    Assert.assertEquals(-1388281606,
                        TypeHasher.hash(OdpsType.INT, ((Short) Short.MIN_VALUE).intValue()));
    Assert.assertEquals(2011537724, TypeHasher.hash(OdpsType.INT, 0x7FFFFFFF));
    Assert.assertEquals(-1789595061, TypeHasher.hash(OdpsType.INT, 0x80000000));
    Assert.assertEquals(-1789595061, TypeHasher.hash(OdpsType.INT, (Integer) Integer.MIN_VALUE));
  }

  @Test
  public void testBigintHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.BIGINT, 0L));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.BIGINT, null));

    Assert.assertEquals(357654460, TypeHasher.hash(OdpsType.BIGINT, 1L));
    Assert.assertEquals(357653185, TypeHasher.hash(OdpsType.BIGINT, -1L));

    Assert.assertEquals(-718433742, TypeHasher.hash(OdpsType.BIGINT, 10L));
    Assert.assertEquals(-718434491, TypeHasher.hash(OdpsType.BIGINT, -10L));

    Assert.assertEquals(-1977349188, TypeHasher.hash(OdpsType.BIGINT, 9223372036854775807L));
    Assert.assertEquals(-1977349188, TypeHasher.hash(OdpsType.BIGINT, Long.MAX_VALUE));
    Assert.assertEquals(-1787473579, TypeHasher.hash(OdpsType.BIGINT, Long.MIN_VALUE));
  }

  @Test
  public void testDateHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.DATE, new java.sql.Date(0)));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.DATE, null));
  }

  @Test
  public void testDatetimeHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.DATETIME, new java.util.Date(0)));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.DATETIME, null));

    Assert.assertEquals(357654460, TypeHasher.hash(OdpsType.DATETIME, new java.util.Date(1)));
    Assert.assertEquals(357653185, TypeHasher.hash(OdpsType.DATETIME, new java.util.Date(-1)));

    Assert.assertEquals(-718433742, TypeHasher.hash(OdpsType.DATETIME, new java.util.Date(10)));
    Assert.assertEquals(-718434491, TypeHasher.hash(OdpsType.DATETIME, new java.util.Date(-10)));

    Assert.assertEquals(-1977349188,
                        TypeHasher.hash(OdpsType.DATETIME, new java.util.Date(Long.MAX_VALUE)));
    Assert.assertEquals(-1787473579,
                        TypeHasher.hash(OdpsType.DATETIME, new java.util.Date(Long.MIN_VALUE)));

    Assert.assertEquals(-1977349188, TypeHasher.hash(OdpsType.DATETIME,
                                                     new java.util.Date(9223372036854775807L)));
  }

  @Test
  public void testBoolHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.BOOLEAN, null));
    Assert.assertEquals(388737479, TypeHasher.hash(OdpsType.BOOLEAN, true));
    Assert.assertEquals(-978963218, TypeHasher.hash(OdpsType.BOOLEAN, false));
  }

  @Test
  public void testFloatHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.FLOAT, 0.0f));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.FLOAT, null));

    Assert.assertEquals(-567953753, TypeHasher.hash(OdpsType.FLOAT, 1.99f));
    Assert.assertEquals(405646127, TypeHasher.hash(OdpsType.FLOAT, -1.99f));

    Assert.assertEquals(1233114570, TypeHasher.hash(OdpsType.FLOAT, 2.001f));
    Assert.assertEquals(368473317, TypeHasher.hash(OdpsType.FLOAT, -2.0001f));

    Assert.assertEquals(278239804, TypeHasher.hash(OdpsType.FLOAT, 3.402823466e+38F));
    Assert.assertEquals(278239804, TypeHasher.hash(OdpsType.FLOAT, Float.MAX_VALUE));
    Assert.assertEquals(1080079518, TypeHasher.hash(OdpsType.FLOAT, 1.175494351e-38F));
    Assert.assertEquals(357654460, TypeHasher.hash(OdpsType.FLOAT, Float.MIN_VALUE));
  }

  @Test
  public void testDoubleHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.DOUBLE, 0.0d));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.DOUBLE, null));

    Assert.assertEquals(-670340059, TypeHasher.hash(OdpsType.DOUBLE, 1.99d));
    Assert.assertEquals(-818909319, TypeHasher.hash(OdpsType.DOUBLE, -1.99d));

    Assert.assertEquals(-1588151741, TypeHasher.hash(OdpsType.DOUBLE, 12.3456d));
    Assert.assertEquals(-282296198, TypeHasher.hash(OdpsType.DOUBLE, -12.3456d));

    Assert.assertEquals(525598140, TypeHasher.hash(OdpsType.DOUBLE, 1.7976931348623158e+308d));
    Assert.assertEquals(525598140, TypeHasher.hash(OdpsType.DOUBLE, Double.MAX_VALUE));

    Assert.assertEquals(-1071819094, TypeHasher.hash(OdpsType.DOUBLE, 2.2250738585072014e-308));
    Assert.assertEquals(357654460, TypeHasher.hash(OdpsType.DOUBLE, Double.MIN_VALUE));
  }

  @Test
  public void testStringHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.STRING, ""));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.STRING, null));

    Assert.assertEquals(-2140438327, TypeHasher.hash(OdpsType.STRING, "1"));
    Assert.assertEquals(-866984865, TypeHasher.hash(OdpsType.STRING, "a1b()2c3"));

    Assert.assertEquals(2003021223, TypeHasher.hash(OdpsType.STRING, "$%ef&*"));
    Assert.assertEquals(1623550194, TypeHasher.hash(OdpsType.STRING, "阿里巴巴云计算"));
    Assert.assertEquals(-1224825444, TypeHasher.hash(OdpsType.STRING, "*^c哦也哈*#"));

    Assert.assertEquals(-967043897, TypeHasher.hash(OdpsType.STRING, "  1  "));
    Assert.assertEquals(-1507343086, TypeHasher.hash(OdpsType.STRING, "1   牛"));
    Assert.assertEquals(966471106, TypeHasher.hash(OdpsType.STRING, "+_+ = ++"));
  }

  @Test
  public void testTimeStampHasher() {
    OdpsHasher timestampHasher = TypeHasher.getHasher(OdpsType.TIMESTAMP);

    Assert.assertEquals(0, timestampHasher.hash(null));

    Timestamp ts = new Timestamp(0);
    ts.setNanos(0);
    java.time.Instant instant = Instant.ofEpochSecond(0, 0);

    Assert.assertEquals(0, timestampHasher.hash(timestampHasher.normalizeType(ts)));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.TIMESTAMP, instant));

    ts = new Timestamp(1 * 1000);
    ts.setNanos(2);
    instant = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    Assert.assertEquals(1508957768, timestampHasher.hash(timestampHasher.normalizeType(ts)));
    Assert.assertEquals(1508957768, TypeHasher.hash(OdpsType.TIMESTAMP, instant));

    ts = new Timestamp(36925 * 1000);
    ts.setNanos(147258369);
    instant = Instant.ofEpochMilli(ts.getTime());
    instant = instant.plusNanos(147258369 - instant.getNano());

    Assert.assertEquals(1469401250, timestampHasher.hash(timestampHasher.normalizeType(ts)));
    Assert.assertEquals(1469401250, TypeHasher.hash(OdpsType.TIMESTAMP, instant));
  }

  @Test
  public void testIntervalDayTimeHasher() {
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.INTERVAL_DAY_TIME, null));
    Assert.assertEquals(0, TypeHasher.hash(OdpsType.INTERVAL_DAY_TIME, new IntervalDayTime(0, 0)));
    Assert.assertEquals(75516668, TypeHasher.hash(OdpsType.INTERVAL_DAY_TIME,
                                                  new IntervalDayTime(123456, 654321)));
    Assert.assertEquals(-1088782317, TypeHasher.hash(OdpsType.INTERVAL_DAY_TIME,
                                                     new IntervalDayTime(100002, 2000001)));
  }

  @Test
  public void TestCombineHasher() {
    int[] hashVals = {0, 0, 0, 0, 0, 0, 0, -978963218, 0, 0, 0, 0, 0};
    Assert.assertEquals(979604186, TypeHasher.CombineHashVal(hashVals));

    int[] hashVals2 = {357654460, 715307540, 1072960876, 357654460, 357654460,
                       357654460, 357654460, 388737479, -567953753, -670340059,
                       -2140438327, 1469401250, -1088782317};
    Assert.assertEquals(966550009, TypeHasher.CombineHashVal(hashVals2));

    int[] hashVals3 = {357653185, 715306435, 1072959681, 357653185, 357653185,
                       357653185, 357653185, 388737479, 405646127, -818909319,
                       -866984865, -64049556, -2054932282};
    Assert.assertEquals(563915101, TypeHasher.CombineHashVal(hashVals3));

    int[] hashVals4 = {-1111111111};
    System.out.println(TypeHasher.CombineHashVal(hashVals4));
  }
}
