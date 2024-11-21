package com.aliyun.odps.data.expression;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TruncTimeTest {

  private static Record record;

  @BeforeClass
  public static void setup() {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("timestamp", TypeInfoFactory.TIMESTAMP));
    schema.addColumn(new Column("date", TypeInfoFactory.DATE));
    schema.addColumn(new Column("datetime", TypeInfoFactory.DATETIME));
    schema.addColumn(new Column("timestamp_ntz", TypeInfoFactory.TIMESTAMP_NTZ));
    record = new ArrayRecord(schema);
  }


  @Test
  public void testGeneratePartitionSpec() {
    TableSchema newSchema =
        TableSchema.builder().withPartitionColumn(Column.newBuilder("p1", TypeInfoFactory.STRING)
                                                      .withGenerateExpression(
                                                          new TruncTime("timestamp", "day"))
                                                      .build()).build();
    record.set(0, Timestamp.from(Instant.now()));
    PartitionSpec partitionSpec = newSchema.generatePartitionSpec(record);
    System.out.println(partitionSpec);
  }

  @Test
  public void testRecordGetterSetter() {
    record.set(0, Timestamp.from(Instant.now()));
    System.out.println(record.get(0).getClass().getName());
    System.out.println(record.get(0));

  }

  @Test
  public void testTruncTimestamp() {
    Timestamp timestamp = Timestamp.from(Instant.parse("2023-12-25T09:09:35Z"));
    System.out.println(timestamp); // 2023-12-25 17:09:35.0
    System.out.println(timestamp.getTime());  // 1703495375000
    record.set(0, timestamp);

    TruncTime truncYear = new TruncTime("timestamp", "year");
    TruncTime truncMonth = new TruncTime("timestamp", "month");
    TruncTime truncDay = new TruncTime("timestamp", "day");
    TruncTime truncHour = new TruncTime("timestamp", "hour");

    System.out.println(truncYear.generate(record));
    System.out.println(truncMonth.generate(record));
    System.out.println(truncDay.generate(record));
    System.out.println(truncHour.generate(record));

    Assert.assertEquals("2023", truncYear.generate(record));
    Assert.assertEquals("2023-12", truncMonth.generate(record));
    Assert.assertEquals("2023-12-25", truncDay.generate(record));
    Assert.assertEquals("2023-12-25 09:00:00", truncHour.generate(record));
  }

  @Test
  public void testTruncInstant() {
    Instant instance = Instant.parse("2023-12-25T09:09:35Z");
    System.out.println(instance); // 2023-12-25T09:09:35Z

    record.set(0, instance);

    TruncTime truncYear = new TruncTime("timestamp", "year");
    TruncTime truncMonth = new TruncTime("timestamp", "month");
    TruncTime truncDay = new TruncTime("timestamp", "day");
    TruncTime truncHour = new TruncTime("timestamp", "hour");

    System.out.println(truncYear.generate(record));
    System.out.println(truncMonth.generate(record));
    System.out.println(truncDay.generate(record));
    System.out.println(truncHour.generate(record));

    Assert.assertEquals("2023", truncYear.generate(record));
    Assert.assertEquals("2023-12", truncMonth.generate(record));
    Assert.assertEquals("2023-12-25", truncDay.generate(record));
    Assert.assertEquals("2023-12-25 09:00:00", truncHour.generate(record));
  }

  @Test
  public void testTruncLocalDateTime() {
    java.time.LocalDateTime localDateTime = java.time.LocalDateTime.of(2023, 12, 25, 9, 9, 35);
    System.out.println(localDateTime); // 2023-12-25T09:09:35
    record.set(3, localDateTime);

    TruncTime truncYear = new TruncTime("timestamp_ntz", "year");
    TruncTime truncMonth = new TruncTime("timestamp_ntz", "month");
    TruncTime truncDay = new TruncTime("timestamp_ntz", "day");
    TruncTime truncHour = new TruncTime("timestamp_ntz", "hour");

    System.out.println(truncYear.generate(record));
    System.out.println(truncMonth.generate(record));
    System.out.println(truncDay.generate(record));
    System.out.println(truncHour.generate(record));

    Assert.assertEquals("2023", truncYear.generate(record));
    Assert.assertEquals("2023-12", truncMonth.generate(record));
    Assert.assertEquals("2023-12-25", truncDay.generate(record));
    Assert.assertEquals("2023-12-25 09:00:00", truncHour.generate(record));
  }

  @Test
  public void testTruncLocalDate() {
    LocalDate localDate = LocalDate.of(2023, 12, 25);
    System.out.println(localDate); // 2023-12-25
    record.set(1, localDate);
    TruncTime truncYear = new TruncTime("date", "year");
    TruncTime truncMonth = new TruncTime("date", "month");
    TruncTime truncDay = new TruncTime("date", "day");

    System.out.println(truncYear.generate(record));
    System.out.println(truncMonth.generate(record));
    System.out.println(truncDay.generate(record));

    Assert.assertEquals("2023", truncYear.generate(record));
    Assert.assertEquals("2023-12", truncMonth.generate(record));
    Assert.assertEquals("2023-12-25", truncDay.generate(record));
  }

  @Test
  public void testTruncSqlDate() {
    Calendar calendar = new Calendar.Builder()
        .setCalendarType("iso8601")
        .setLenient(true)
        .setTimeZone(TimeZone.getTimeZone("GMT"))
        .build();
    calendar.set(2023, 12 - 1, 25);
    java.sql.Date date = new java.sql.Date(calendar.getTimeInMillis());
    System.out.println(date);

    record.set(1, date);
    TruncTime truncYear = new TruncTime("date", "year");
    TruncTime truncMonth = new TruncTime("date", "month");
    TruncTime truncDay = new TruncTime("date", "day");

    System.out.println(truncYear.generate(record));
    System.out.println(truncMonth.generate(record));
    System.out.println(truncDay.generate(record));

    Assert.assertEquals("2023", truncYear.generate(record));
    Assert.assertEquals("2023-12", truncMonth.generate(record));
    Assert.assertEquals("2023-12-25", truncDay.generate(record));
  }

  @Test
  public void testTruncUtilDate() {
    java.util.Date date = new java.util.Date(1703495375000L);
    System.out.println(date); // Mon Dec 25 17:09:35 CST 2023

    record.set(2, date);
    TruncTime truncYear = new TruncTime("datetime", "year");
    TruncTime truncMonth = new TruncTime("datetime", "month");
    TruncTime truncDay = new TruncTime("datetime", "day");
    TruncTime truncHour = new TruncTime("datetime", "hour");

    System.out.println(truncYear.generate(record));
    System.out.println(truncMonth.generate(record));
    System.out.println(truncDay.generate(record));
    System.out.println(truncHour.generate(record));

    Assert.assertEquals("2023", truncYear.generate(record));
    Assert.assertEquals("2023-12", truncMonth.generate(record));
    Assert.assertEquals("2023-12-25", truncDay.generate(record));
    Assert.assertEquals("2023-12-25 09:00:00", truncHour.generate(record));
  }

  @Test
  public void testTruncZonedDateTime() {
    ZonedDateTime
        zonedDateTime =
        Instant.parse("2023-12-25T09:09:35Z").atZone(ZoneId.systemDefault());
    System.out.println(zonedDateTime); // 2023-12-25T17:09:35+08:00[Asia/Shanghai]

    record.set(2, zonedDateTime);
    TruncTime truncYear = new TruncTime("datetime", "year");
    TruncTime truncMonth = new TruncTime("datetime", "month");
    TruncTime truncDay = new TruncTime("datetime", "day");
    TruncTime truncHour = new TruncTime("datetime", "hour");

    System.out.println(truncYear.generate(record));
    System.out.println(truncMonth.generate(record));
    System.out.println(truncDay.generate(record));
    System.out.println(truncHour.generate(record));

    Assert.assertEquals("2023", truncYear.generate(record));
    Assert.assertEquals("2023-12", truncMonth.generate(record));
    Assert.assertEquals("2023-12-25", truncDay.generate(record));
    Assert.assertEquals("2023-12-25 09:00:00", truncHour.generate(record));
  }

  @Test
  public void testNullValue() {
    record.set(0, null);
    TruncTime truncYear = new TruncTime("timestamp", "year");
    String generate = truncYear.generate(record);
    Assert.assertEquals("__NULL__", generate);
  }

  @Test
  public void testTimeBefore1960() {
    ZonedDateTime
        zonedDateTime =
        Instant.parse("1900-12-25T09:09:35Z").atZone(ZoneId.systemDefault());
    System.out.println(zonedDateTime); // 1900-12-25T17:09:35+08:00[Asia/Shanghai]

    record.set(2, zonedDateTime);
    TruncTime truncYear = new TruncTime("datetime", "year");
    TruncTime truncMonth = new TruncTime("datetime", "month");
    TruncTime truncDay = new TruncTime("datetime", "day");
    TruncTime truncHour = new TruncTime("datetime", "hour");

    System.out.println(truncYear.generate(record));
    System.out.println(truncMonth.generate(record));
    System.out.println(truncDay.generate(record));
    System.out.println(truncHour.generate(record));

    Assert.assertEquals("1959", truncYear.generate(record));
    Assert.assertEquals("1959-12", truncMonth.generate(record));
    Assert.assertEquals("1959-12-31", truncDay.generate(record));
    Assert.assertEquals("1959-12-31 23:00:00", truncHour.generate(record));
  }

}
