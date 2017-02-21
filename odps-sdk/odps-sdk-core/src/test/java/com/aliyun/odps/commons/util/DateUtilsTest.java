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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.TestBase;

public class DateUtilsTest extends TestBase {

  private static Calendar c1 = Calendar.getInstance();    //0001-01-01 00:00:00
  private static Calendar c2 = Calendar.getInstance();    //1927-12-31 23:54:07
  private static Calendar c3 = Calendar.getInstance();    //1928-01-01 00:00:00
  private static Calendar c4 = Calendar.getInstance();    //1970-01-01 00:00:00
  private static Calendar c5 = Calendar.getInstance();    //1997-08-31 23:59:59
  private static Calendar c6 = Calendar.getInstance();    //1997-09-01 00:59:59
  private static Calendar c7 = Calendar.getInstance();    //2000-02-29 23:59:59
  private static Calendar c8 = Calendar.getInstance();    //2014-08-26 15:22:46
  private static Calendar c9 = Calendar.getInstance();    //2018-11-30 23:59:59
  private static Calendar c10 = Calendar.getInstance();   //2499-01-01 00:00:00
  private static Calendar c11 = Calendar.getInstance();   //1986-05-04 01:00:00
  private static Calendar c12 = Calendar.getInstance();   //1986-05-04 02:00:00

  private static long __0001_01_01_00_00_00 = -62135625952L;
  private static long __1927_12_31_23_54_07 = -1325491905L;
  private static long __1928_01_01_00_00_00 = -1325491200L;
  private static long __1970_01_01_00_00_00 = -28800L;
  private static long __1997_08_31_23_59_59 = 873043199L;
  private static long __1997_09_01_00_59_59 = 873046799L;
  private static long __2000_02_29_23_59_59 = 951839999L;
  private static long __2014_08_26_15_22_46 = 1409037766L;
  private static long __2018_11_30_23_59_59 = 1543593599L;
  private static long __2499_01_01_00_00_00 = 16693660800L;
  private static long __1986_05_04_01_00_00 = 515520000L;
  private static long __1986_05_04_02_00_00 = 515523600L;

  @Before
  public void setUp() throws Exception {
    c1.set(Calendar.YEAR, 1);
    c1.set(Calendar.MONTH, 1 - 1);
    c1.set(Calendar.DAY_OF_MONTH, 1);
    c1.set(Calendar.HOUR_OF_DAY, 0);
    c1.set(Calendar.MINUTE, 0);
    c1.set(Calendar.SECOND, 0);
    c1.set(Calendar.MILLISECOND, 0);

    c2.set(Calendar.YEAR, 1927);
    c2.set(Calendar.MONTH, 12 - 1);
    c2.set(Calendar.DAY_OF_MONTH, 31);
    c2.set(Calendar.HOUR_OF_DAY, 23);
    c2.set(Calendar.MINUTE, 54);
    c2.set(Calendar.SECOND, 7);
    c2.set(Calendar.MILLISECOND, 0);

    c3.set(Calendar.YEAR, 1928);
    c3.set(Calendar.MONTH, 1 - 1);
    c3.set(Calendar.DAY_OF_MONTH, 1);
    c3.set(Calendar.HOUR_OF_DAY, 0);
    c3.set(Calendar.MINUTE, 0);
    c3.set(Calendar.SECOND, 0);
    c3.set(Calendar.MILLISECOND, 0);

    c4.set(Calendar.YEAR, 1970);
    c4.set(Calendar.MONTH, 1 - 1);
    c4.set(Calendar.DAY_OF_MONTH, 1);
    c4.set(Calendar.HOUR_OF_DAY, 0);
    c4.set(Calendar.MINUTE, 0);
    c4.set(Calendar.SECOND, 0);
    c4.set(Calendar.MILLISECOND, 0);

    c5.set(Calendar.YEAR, 1997);
    c5.set(Calendar.MONTH, 8 - 1);
    c5.set(Calendar.DAY_OF_MONTH, 31);
    c5.set(Calendar.HOUR_OF_DAY, 23);
    c5.set(Calendar.MINUTE, 59);
    c5.set(Calendar.SECOND, 59);
    c5.set(Calendar.MILLISECOND, 0);

    c6.set(Calendar.YEAR, 1997);
    c6.set(Calendar.MONTH, 9 - 1);
    c6.set(Calendar.DAY_OF_MONTH, 1);
    c6.set(Calendar.HOUR_OF_DAY, 0);
    c6.set(Calendar.MINUTE, 59);
    c6.set(Calendar.SECOND, 59);
    c6.set(Calendar.MILLISECOND, 0);

    c7.set(Calendar.YEAR, 2000);
    c7.set(Calendar.MONTH, 2 - 1);
    c7.set(Calendar.DAY_OF_MONTH, 29);
    c7.set(Calendar.HOUR_OF_DAY, 23);
    c7.set(Calendar.MINUTE, 59);
    c7.set(Calendar.SECOND, 59);
    c7.set(Calendar.MILLISECOND, 0);

    c8.set(Calendar.YEAR, 2014);
    c8.set(Calendar.MONTH, 8 - 1);
    c8.set(Calendar.DAY_OF_MONTH, 26);
    c8.set(Calendar.HOUR_OF_DAY, 15);
    c8.set(Calendar.MINUTE, 22);
    c8.set(Calendar.SECOND, 46);
    c8.set(Calendar.MILLISECOND, 0);

    c9.set(Calendar.YEAR, 2018);
    c9.set(Calendar.MONTH, 11 - 1);
    c9.set(Calendar.DAY_OF_MONTH, 30);
    c9.set(Calendar.HOUR_OF_DAY, 23);
    c9.set(Calendar.MINUTE, 59);
    c9.set(Calendar.SECOND, 59);
    c9.set(Calendar.MILLISECOND, 0);

    c10.set(Calendar.YEAR, 2499);
    c10.set(Calendar.MONTH, 1 - 1);
    c10.set(Calendar.DAY_OF_MONTH, 1);
    c10.set(Calendar.HOUR_OF_DAY, 0);
    c10.set(Calendar.MINUTE, 0);
    c10.set(Calendar.SECOND, 0);
    c10.set(Calendar.MILLISECOND, 0);

    c11.set(Calendar.YEAR, 1986);
    c11.set(Calendar.MONTH, 5 - 1);
    c11.set(Calendar.DAY_OF_MONTH, 4);
    c11.set(Calendar.HOUR_OF_DAY, 1);
    c11.set(Calendar.MINUTE, 0);
    c11.set(Calendar.SECOND, 0);
    c11.set(Calendar.MILLISECOND, 0);

    c12.set(Calendar.YEAR, 1986);
    c12.set(Calendar.MONTH, 5 - 1);
    c12.set(Calendar.DAY_OF_MONTH, 4);
    c12.set(Calendar.HOUR_OF_DAY, 2);
    c12.set(Calendar.MINUTE, 0);
    c12.set(Calendar.SECOND, 0);
    c12.set(Calendar.MILLISECOND, 0);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testRawtime2date() throws IOException {
    Assert.assertEquals(DateUtils.rawtime2date(__0001_01_01_00_00_00), c1.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__1927_12_31_23_54_07), c2.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__1928_01_01_00_00_00), c3.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__1970_01_01_00_00_00), c4.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__1997_08_31_23_59_59), c5.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__1997_09_01_00_59_59), c6.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__2000_02_29_23_59_59), c7.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__2014_08_26_15_22_46), c8.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__2018_11_30_23_59_59), c9.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__2499_01_01_00_00_00), c10.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__1986_05_04_01_00_00), c11.getTime());
    Assert.assertEquals(DateUtils.rawtime2date(__1986_05_04_02_00_00), c12.getTime());
  }

  @Test
  public void testDate2rawtime() throws IOException {
    Assert.assertEquals(DateUtils.date2rawtime(c1.getTime()), __0001_01_01_00_00_00);
    Assert.assertEquals(DateUtils.date2rawtime(c2.getTime()), __1927_12_31_23_54_07);
    Assert.assertEquals(DateUtils.date2rawtime(c3.getTime()), __1928_01_01_00_00_00);
    Assert.assertEquals(DateUtils.date2rawtime(c4.getTime()), __1970_01_01_00_00_00);
    Assert.assertEquals(DateUtils.date2rawtime(c5.getTime()), __1997_08_31_23_59_59);
    Assert.assertEquals(DateUtils.date2rawtime(c6.getTime()), __1997_09_01_00_59_59);
    Assert.assertEquals(DateUtils.date2rawtime(c7.getTime()), __2000_02_29_23_59_59);
    Assert.assertEquals(DateUtils.date2rawtime(c8.getTime()), __2014_08_26_15_22_46);
    Assert.assertEquals(DateUtils.date2rawtime(c9.getTime()), __2018_11_30_23_59_59);
    Assert.assertEquals(DateUtils.date2rawtime(c10.getTime()), __2499_01_01_00_00_00);
    Assert.assertEquals(DateUtils.date2rawtime(c11.getTime()), __1986_05_04_01_00_00);
    Assert.assertEquals(DateUtils.date2rawtime(c12.getTime()), __1986_05_04_02_00_00);
  }

  @Test
  public void testEquality() throws IOException {
    Assert.assertEquals(c1.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c1.getTime())));
    Assert.assertEquals(c2.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c2.getTime())));
    Assert.assertEquals(c3.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c3.getTime())));
    Assert.assertEquals(c4.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c4.getTime())));
    Assert.assertEquals(c5.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c5.getTime())));
    Assert.assertEquals(c6.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c6.getTime())));
    Assert.assertEquals(c7.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c7.getTime())));
    Assert.assertEquals(c8.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c8.getTime())));
    Assert.assertEquals(c9.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c9.getTime())));
    Assert
        .assertEquals(c10.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c10.getTime())));
    Assert
        .assertEquals(c11.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c11.getTime())));
    Assert
        .assertEquals(c12.getTime(), DateUtils.rawtime2date(DateUtils.date2rawtime(c12.getTime())));
  }

  @Ignore
  @Test
  public void testUTCOffset() throws IOException {
    Calendar cal = Calendar.getInstance();
    cal.clear();
    cal.set(9999, 0, 1, 0, 0, 0); // 9999-1-1
    long lastTime = cal.getTimeInMillis();
    cal.clear();
    cal.set(0, 0, 1, 0, 0, 0); // 0001-1-1
    long startTime = cal.getTimeInMillis();
    long t = startTime;

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    while (t <= lastTime) {
      java.sql.Date d = java.sql.Date.valueOf(format.format(t));
      long off = DateUtils.getDayOffset(d);
      java.sql.Date resD = DateUtils.fromDayOffset(off);
      Assert.assertEquals(d, resD);
      cal.add(Calendar.DATE, 1);
      t = cal.getTimeInMillis();
    }
  }
}
