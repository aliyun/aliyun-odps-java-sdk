/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.commons.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import com.aliyun.odps.utils.StringUtils;

public class DateUtils {

  private static long TZ = +8;
  public static Calendar LOCAL_CAL = Calendar.getInstance();
  public static Calendar SHANGHAI_CAL = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));

  private static Calendar CAL = SHANGHAI_CAL;

  private static Calendar GMT_CAL = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
  private static long MILLIS_OF_DAY = 24 * 60 * 60 * 1000;

  //Java unix time stamp of Current Timezone
  private static final long _0001_01_01 = getTime(1, 1, 1, 0, 0, 0);
  private static final long _0000_03_01 = getTime(0, 3, 1, 0, 0, 0);
  private static final long _1928_01_01 = getTime(1928, 1, 1, 0, 0, 0);
  private static final long _1940_06_03_01 = getTime(1940, 6, 3, 1, 0, 0);
  private static final long _1940_10_01 = getTime(1940, 10, 1, 0, 0, 0);
  private static final long _1941_03_16_01 = getTime(1941, 3, 16, 1, 0, 0);
  private static final long _1941_10_01 = getTime(1941, 10, 1, 0, 0, 0);
  private static final long _1986_05_04_01 = getTime(1986, 5, 4, 1, 0, 0);
  private static final long _1986_09_14 = getTime(1986, 9, 14, 0, 0, 0);
  private static final long _1987_04_12_01 = getTime(1987, 4, 12, 1, 0, 0);
  private static final long _1987_09_13 = getTime(1987, 9, 13, 0, 0, 0);
  private static final long _1988_04_10_01 = getTime(1988, 4, 10, 1, 0, 0);
  private static final long _1988_09_11 = getTime(1988, 9, 11, 0, 0, 0);
  private static final long _1989_04_16_01 = getTime(1989, 4, 16, 1, 0, 0);
  private static final long _1989_09_17 = getTime(1989, 9, 17, 0, 0, 0);
  private static final long _1990_04_15_01 = getTime(1990, 4, 15, 1, 0, 0);
  private static final long _1990_09_16 = getTime(1990, 9, 16, 0, 0, 0);
  private static final long _1991_04_14_01 = getTime(1991, 4, 14, 1, 0, 0);
  private static final long _1991_09_15 = getTime(1991, 9, 15, 0, 0, 0);

  //C unix time stamp of CST
  private static final long _0000_01_01_C = -62167248352L;
  private static final long _0000_03_01_C = -62162064352L;
  private static final long _0000_03_01_08_C = -62162035552L;
  private static final long _1927_12_31_23_59_59_C = -1325491553L;
  private static final long _1940_06_03_01_C = -933494400L;
  private static final long _1940_09_30_23_C = -923130000L;
  private static final long _1941_03_16_01_C = -908784000L;
  private static final long _1941_09_30_23_C = -891594000L;
  private static final long _1986_05_04_01_C = 515520000L;
  private static final long _1986_09_13_23_C = 527007600L;
  private static final long _1987_04_12_01_C = 545155200L;
  private static final long _1987_09_12_23_C = 558457200L;
  private static final long _1988_04_10_01_C = 576604800L;
  private static final long _1988_09_10_23_C = 589906800L;
  private static final long _1989_04_16_01_C = 608659200L;
  private static final long _1989_09_16_23_C = 621961200L;
  private static final long _1990_04_15_01_C = 640108800L;
  private static final long _1990_09_15_23_C = 653410800L;
  private static final long _1991_04_14_01_C = 671558400L;
  private static final long _1991_09_14_23_C = 684860400L;

  private static long getTime(int year, int month, int day, int hour, int min, int sec) {
    Calendar c = (Calendar) CAL.clone();


    c.set(Calendar.YEAR, year);
    c.set(Calendar.MONTH, month - 1);
    c.set(Calendar.DAY_OF_MONTH, day);
    c.set(Calendar.HOUR_OF_DAY, hour);
    c.set(Calendar.MINUTE, min);
    c.set(Calendar.SECOND, sec);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTime().getTime();
  }

  public static long date2ms(Date date, Calendar calendar) {
    if (calendar == null) {
      calendar = SHANGHAI_CAL;
    }

    long ms;
    Calendar c = null;

    c = (Calendar) calendar.clone();
    c.setTime(date);
    ms = c.get(Calendar.MILLISECOND);

    return date2rawtime(date, calendar) * 1000 + ms;
  }

  public static long date2ms(Date date) {
    return date2ms(date, SHANGHAI_CAL);
  }

  public static Date ms2date(long ms) {
    return ms2date(ms, SHANGHAI_CAL);
  }
  
  public static Date ms2date(long ms, Calendar calendar) {
    if (calendar == null) {
      calendar = SHANGHAI_CAL;
    }

    Date d = rawtime2date(ms / 1000, calendar);
    Calendar c = (Calendar) calendar.clone();

    c.setTime(d);
    c.set(Calendar.MILLISECOND, (int) (ms % 1000));

    return c.getTime();
  }

  /**
   * @param date
   *     of Date Class
   * @return Unix Time Stamp
   * @brief Java version of GLIBC mktime function
   *
   * 1. This algorithm is design to convert date to unix timestamp.
   * The date "0000-03-01 00:00:00" is regarded as the beginning and other
   * dates are computed based on it.
   * 2. There is no parameter verification(assuming the parameter is legal
   * and unambiguous).
   */
  public static long date2rawtime(Date date) {
    return date2rawtime(date, SHANGHAI_CAL);
  }

  public static long date2rawtime(Date date, Calendar calendar) {
    //no input parameter verification
    //get literal value of broken-down time regard less of time zone
    Calendar c = null;
    long rawtime;
    int year, mon, day, hour, min, sec;
    long ans;

    c = (Calendar) calendar.clone();
    c.setTime(date);
    c.set(Calendar.MILLISECOND, 0);

    rawtime = date.getTime();
    year = c.get(Calendar.YEAR);
    if (rawtime < _0001_01_01) {
      year = 0;
    }
    mon = c.get(Calendar.MONTH) + 1;
    day = c.get(Calendar.DAY_OF_MONTH);
    hour = c.get(Calendar.HOUR_OF_DAY);
    min = c.get(Calendar.MINUTE);
    sec = c.get(Calendar.SECOND);

    if (rawtime < _0000_03_01) {
      return _0000_01_01_C + ((((mon - 1) * 31 + (day - 1)) * 24 + hour) * 60 + min) * 60 + sec;
    }

    //get literal value of calendar time regard less of time zone
    mon = mon - 2;
    if (mon <= 0) {
      mon += 12;
      year -= 1;
    }
    ans = year / 4 - year / 100 + year / 400 + 367 * mon / 12 + day;
    ans +=
        (year * 365
         - 719499);   //719499 is a gap between 0000-03-01 00:00:00 and 1970-01-01 00:00:00 plus compensation of month
    ans = (((ans * 24 + hour) * 60 + min) * 60) + sec;

    //adjust for time zone
    ans = ans - TZ * 3600;

    //adjust for history
    if (rawtime < _1928_01_01) {
      ans = ans - 352;
    } else if (rawtime >= _1940_06_03_01 && rawtime < _1940_10_01) {
      ans = ans - 3600;
    } else if (rawtime >= _1941_03_16_01 && rawtime < _1941_10_01) {
      ans = ans - 3600;
    } else if (rawtime >= _1986_05_04_01 && rawtime < _1986_09_14) {
      ans = ans - 3600;
    } else if (rawtime >= _1987_04_12_01 && rawtime < _1987_09_13) {
      ans = ans - 3600;
    } else if (rawtime >= _1988_04_10_01 && rawtime < _1988_09_11) {
      ans = ans - 3600;
    } else if (rawtime >= _1989_04_16_01 && rawtime < _1989_09_17) {
      ans = ans - 3600;
    } else if (rawtime >= _1990_04_15_01 && rawtime < _1990_09_16) {
      ans = ans - 3600;
    } else if (rawtime >= _1991_04_14_01 && rawtime < _1991_09_15) {
      ans = ans - 3600;
    }

    return ans;
  }

  /**
   * @param rawtime
   *     Unix Time Stamp
   * @return Object of Date Class
   * @brief Java version of GLIBC localtime function
   *
   * 1. The algorithm is a reverse of mktime.
   * 2. There is no parameter verification(assuming the parameter is legal
   * and unambiguous).
   */
  public static Date rawtime2date(long rawtime) {
    return rawtime2date(rawtime, SHANGHAI_CAL);
  }

  public static Date rawtime2date(long rawtime, Calendar calendar) {
    int year, mon, day, hour, min, sec, leap;
    long offset;

    Calendar c = (Calendar) calendar.clone();

    if (rawtime < _0000_03_01_C) {
      offset = rawtime - _0000_01_01_C;
      sec = (int) (offset % 60);
      offset /= 60;
      min = (int) (offset % 60);
      offset /= 60;
      hour = (int) (offset % 24);
      offset /= 24;
      mon = (int) ((offset / 31) + 1);
      offset = offset % 31;
      day = (int) (offset + 1);
      year = 0;
    } else {
      offset = rawtime - _0000_03_01_08_C;
      if (rawtime > _1927_12_31_23_59_59_C) {
        offset = offset - 352;
      }
      if (rawtime >= _1940_06_03_01_C && rawtime < _1940_09_30_23_C) {
        offset = offset + 3600;
      } else if (rawtime >= _1941_03_16_01_C && rawtime < _1941_09_30_23_C) {
        offset = offset + 3600;
      } else if (rawtime >= _1986_05_04_01_C && rawtime < _1986_09_13_23_C) {
        offset = offset + 3600;
      } else if (rawtime >= _1987_04_12_01_C && rawtime < _1987_09_12_23_C) {
        offset = offset + 3600;
      } else if (rawtime >= _1988_04_10_01_C && rawtime < _1988_09_10_23_C) {
        offset = offset + 3600;
      } else if (rawtime >= _1989_04_16_01_C && rawtime < _1989_09_16_23_C) {
        offset = offset + 3600;
      } else if (rawtime >= _1990_04_15_01_C && rawtime < _1990_09_15_23_C) {
        offset = offset + 3600;
      } else if (rawtime >= _1991_04_14_01_C && rawtime < _1991_09_14_23_C) {
        offset = offset + 3600;
      }
      offset = offset + TZ * 60 * 60;

      sec = (int) (offset % 60);
      offset /= 60;
      min = (int) (offset % 60);
      offset /= 60;
      hour = (int) (offset % 24);
      offset /= 24;

      year = (int) (offset / 365);
      leap = year / 4 - year / 100 + year / 400;
      while (year * 365 + leap > offset) {
        year = year - 1;
        leap = year / 4 - year / 100 + year / 400;
      }
      offset = offset - (year * 365 + leap);

      int i = 12;
      while (offset < 367 * i / 12 - 30) {
        i--;
      }
      offset = offset - (367 * i / 12 - 30);
      mon = i + 2;
      if (mon > 12) {
        mon = mon - 12;
        year = year + 1;
      }

      day = (int) (offset + 1);
    }

    c.set(Calendar.YEAR, year);
    c.set(Calendar.MONTH, mon - 1);
    c.set(Calendar.DAY_OF_MONTH, day);
    c.set(Calendar.HOUR_OF_DAY, hour);
    c.set(Calendar.MINUTE, min);
    c.set(Calendar.SECOND, sec);
    c.set(Calendar.MILLISECOND, 0);

    return c.getTime();
  }

  // RFC 822 Date Format
  private static final String RFC822_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z";

  /**
   * Formats Date to GMT string.
   *
   * @param date
   * @return
   */
  public static String formatRfc822Date(Date date) {
    return getRfc822DateFormat().format(date);
  }

  /**
   * Parses a GMT-format string.
   *
   * @param dateString
   * @return
   * @throws ParseException
   */
  public static Date parseRfc822Date(String dateString) throws ParseException {
    return getRfc822DateFormat().parse(dateString);
  }

  private static DateFormat getRfc822DateFormat() {
    SimpleDateFormat rfc822DateFormat = new SimpleDateFormat(
        RFC822_DATE_FORMAT, Locale.US);
    rfc822DateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));

    return rfc822DateFormat;
  }

  /**
   * This method is error-prone and deprecated.
   */
  @Deprecated
  public static long getDayOffset(java.sql.Date date) {
    Calendar gmtCal = (Calendar) GMT_CAL.clone();
    gmtCal.clear();

    gmtCal.setTime(date);
    // to clip the hour, min, second
    gmtCal.set(gmtCal.get(Calendar.YEAR), gmtCal.get(Calendar.MONTH), gmtCal.get(Calendar.DATE),
             0, 0, 0);
    return gmtCal.getTimeInMillis() / MILLIS_OF_DAY;
  }

  /**
   * This method is error-prone and deprecated.
   */
  @Deprecated
  public static java.sql.Date fromDayOffset(long offset) {
    Calendar gmtCal = (Calendar) GMT_CAL.clone();
    gmtCal.clear();

    gmtCal.setTimeInMillis(offset *  MILLIS_OF_DAY);

    return new java.sql.Date(gmtCal.getTimeInMillis());
  }
}
