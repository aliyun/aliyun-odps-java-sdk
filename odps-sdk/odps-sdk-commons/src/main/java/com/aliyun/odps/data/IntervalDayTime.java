package com.aliyun.odps.data;

import java.util.concurrent.TimeUnit;


/**
 * Interval Day Time 类型对应的数据类
 *
 * Created by zhenhong.gzh on 16/12/12.
 */
public class IntervalDayTime implements Comparable<IntervalDayTime> {
  private final static int NANOS_PER_SECOND = 1000000000;

  protected long totalSeconds;
  protected int nanos;

  public IntervalDayTime(long totalSeconds, int nanos) {
    if (nanos != 0) {
      long secondsFromNano = nanos / NANOS_PER_SECOND;
      nanos %= NANOS_PER_SECOND;
      totalSeconds += secondsFromNano;
    }

    this.totalSeconds = totalSeconds;
    this.nanos = nanos;

    if (totalSeconds > 0 && nanos < 0) {
      --totalSeconds;
      nanos += NANOS_PER_SECOND;
    } else if (totalSeconds < 0 && nanos > 0) {
      ++totalSeconds;
      nanos -= NANOS_PER_SECOND;
    }
  }

  public long getTotalSeconds() {
    return totalSeconds;
  }

  public int getNanos() {
    return nanos;
  }

  public int getDays() {
    return (int)TimeUnit.SECONDS.toDays(totalSeconds);
  }

  public int getHours() {
    return (int)(TimeUnit.SECONDS.toHours(totalSeconds) % TimeUnit.DAYS.toHours(1));
  }

  public int getMinutes() {
    return (int)(TimeUnit.SECONDS.toMinutes(totalSeconds) % TimeUnit.HOURS.toMinutes(1));
  }

  public int getSeconds() {
    return (int)(totalSeconds % TimeUnit.MINUTES.toSeconds(1));
  }

  @Override
  public int compareTo(IntervalDayTime o) {
    long diffSeconds = totalSeconds - o.totalSeconds;
    return diffSeconds < 0 ? -1 : diffSeconds == 0 ? nanos - o.nanos : 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IntervalDayTime that = (IntervalDayTime) o;

    if (totalSeconds != that.totalSeconds) return false;
    return nanos == that.nanos;
  }

  @Override
  public int hashCode() {
    int result = (int) (totalSeconds ^ (totalSeconds >>> 32));
    result = 31 * result + nanos;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    if (totalSeconds < 0 || nanos < 0) {
      buf.append('-');
    }

    int days = getDays();
    if (days < 0) {
      days = -days;
    }
    int hours = getHours();
    if (hours < 0) {
      hours = -hours;
    }
    int minutes = getMinutes();
    if (minutes < 0) {
      minutes = - minutes;
    }
    int seconds = getSeconds();
    if (seconds < 0) {
      seconds = -seconds;
    }
    int nanos = getNanos();
    if (nanos < 0) {
      nanos = -nanos;
    }

    buf.append(days);
    buf.append(String.format(" %02d:%02d:%02d.%09d",
                             hours,
                             minutes,
                             seconds,
                             nanos));
    return buf.toString();
  }
}
