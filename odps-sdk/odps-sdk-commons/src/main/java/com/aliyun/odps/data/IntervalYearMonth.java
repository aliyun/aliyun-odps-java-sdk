package com.aliyun.odps.data;

/**
 * Interval Year Month 类型对应的数据类
 *
 * Created by zhenhong.gzh on 16/12/12.
 */

public class IntervalYearMonth implements Comparable<IntervalYearMonth> {
  private final static int MONTHS_PER_YEAR = 12;
  private final static int MAX_YEARS = 9999;

  protected int totalMonths;

  public IntervalYearMonth(int years, int months) {
    if (Math.abs(totalMonths) / MONTHS_PER_YEAR > MAX_YEARS) {
      throw new IllegalArgumentException(Integer.toString(years));
    }

    if (Math.abs(months) >= MONTHS_PER_YEAR) {
      throw new IllegalArgumentException(Integer.toString(months));
    }

    totalMonths = years * MONTHS_PER_YEAR + months;
  }

  public IntervalYearMonth(int totalMonths) {
    if (Math.abs(totalMonths) / MONTHS_PER_YEAR > MAX_YEARS) {
      throw new IllegalArgumentException(Integer.toString(totalMonths));
    }
    this.totalMonths = totalMonths;
  }

  public int getTotalMonths() {
    return totalMonths;
  }

  public int getYears() {
    return totalMonths / MONTHS_PER_YEAR;
  }

  public int getMonths() {
    return totalMonths % MONTHS_PER_YEAR;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    if (totalMonths < 0) {
      buf.append('-');
    }
    buf.append(Math.abs(getYears())).append('-').append(Math.abs(getMonths()));
    return buf.toString();
  }

  @Override
  public int compareTo(IntervalYearMonth o) {
    return totalMonths - o.totalMonths;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (o == null || getClass() != o.getClass())
      return false;

    IntervalYearMonth that = (IntervalYearMonth) o;

    return totalMonths == that.totalMonths;
  }

  @Override
  public int hashCode() {
    return totalMonths;
  }
}
