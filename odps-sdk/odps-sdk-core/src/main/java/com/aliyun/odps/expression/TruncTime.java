package com.aliyun.odps.expression;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.aliyun.odps.data.GenerateExpression;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.OdpsCommonUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TruncTime implements GenerateExpression {

  public static final String NAME = "trunc_time";
  private static final String NULL_VALUE = "__NULL__";

  /**
   * The timestamp of '1960-01-01'
   * If the time after trunc is less than 1960-01-01, it will return according to the value of datepart:
   * '1959' or '1959-12' or '1959-12-31' or '1959-12-31 23:00:00 '
   */
  private static final long MIN_EPOCH_MILLIS = -315619200000L;

  private final String dateColumnName;
  private final DatePart datePart;
  private DateTimeFormatter formatter;

  public enum DatePart {
    YEAR,
    MONTH,
    DAY,
    HOUR
  }

  public TruncTime(String dateColumnName, DatePart datePart) {
    Preconditions.checkString(dateColumnName, "dateColumnName in trunc_time expression");

    this.dateColumnName = dateColumnName;
    this.datePart = datePart;

    initFormatter();
  }

  public TruncTime(String dateColumnName, String constant) {
    Preconditions.checkString(constant, "dateColumnName in trunc_time expression");
    Preconditions.checkString(dateColumnName, "dateColumnName in trunc_time expression");

    this.dateColumnName = dateColumnName;
    this.datePart = DatePart.valueOf(constant.toUpperCase());

    initFormatter();
  }

  private void initFormatter() {
    switch (datePart) {
      case YEAR:
        formatter = DateTimeFormatter.ofPattern("yyyy");
        break;
      case MONTH:
        formatter = DateTimeFormatter.ofPattern("yyyy-MM");
        break;
      case DAY:
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        break;
      case HOUR:
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        break;
      default:
        throw new IllegalArgumentException("Unknown date part: " + datePart);
    }
  }

  @Override
  public String toString() {
    // eg. "trunc_time(`d`, 'day')"
    return NAME + "(" + OdpsCommonUtils.quoteRef(dateColumnName) + ", '" + datePart.name() + "')";
  }

  @Override
  public String generate(Record record) {
    Object data = record.get(dateColumnName);
    if (data == null) {
      return NULL_VALUE;
    }
    if (data instanceof Timestamp) {
      return truncEpochMillis(((Timestamp) data).getTime());
    } else if (data instanceof Instant) {
      return truncEpochMillis(((Instant) data).toEpochMilli());
    } else if (data instanceof LocalDateTime) {
      return truncEpochMillis(((LocalDateTime) data).toEpochSecond(ZoneOffset.UTC) * 1000);
    } else if (data instanceof ZonedDateTime) {
      return truncEpochMillis(((ZonedDateTime) data).toEpochSecond() * 1000);
    } else if (data instanceof Date) {
      return truncEpochMillis(((Date) data).getTime());
    } else if (data instanceof java.util.Date) {
      return truncEpochMillis(((java.util.Date) data).getTime());
    } else if (data instanceof LocalDate) {
      return truncEpochMillis(((LocalDate) data).toEpochDay() * 86400 * 1000);
    } else {
      throw new IllegalArgumentException("Unknown data type: " + data.getClass());
    }
  }

  private String truncEpochMillis(long epochMillis) {
    if (epochMillis < MIN_EPOCH_MILLIS) {
      return minGenerateValue();
    }
    LocalDateTime
        localDateTime =
        LocalDateTime.ofEpochSecond(epochMillis / 1000, 0, ZoneOffset.UTC);
    localDateTime = localDateTime.withMinute(0).withSecond(0);
    return localDateTime.format(formatter);
  }

  private String minGenerateValue() {
    switch (datePart) {
      case YEAR:
        return "1959";
      case MONTH:
        return "1959-12";
      case DAY:
        return "1959-12-31";
      case HOUR:
        return "1959-12-31 23:00:00";
      default:
        throw new IllegalArgumentException("Unknown date part: " + datePart);
    }
  }
}
