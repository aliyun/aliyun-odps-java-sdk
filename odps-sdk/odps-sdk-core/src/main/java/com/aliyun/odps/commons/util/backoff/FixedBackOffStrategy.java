package com.aliyun.odps.commons.util.backoff;

/**
 * Created by zhenhong.gzh on 17/6/19.
 */
public class FixedBackOffStrategy extends BackOffStrategy {

  private long interval;
  private long startTime = -1;

  /**
   * 常数回避策略：开始时间到下次重试之间的间隔是一个常数
   *
   * 注意本策略的常数等待间隔是以开始时间为基准点的。
   * 在使用时，需要提前调用 {@link #setStartTime(long)} 来设置开始时间。
   *
   * 若在回避时，距离开始时间已经超过常数间隔，则无需等待。
   *
   * @param initInterval
   *     初始间隔
   */
  public FixedBackOffStrategy(long initInterval) {
    super(initInterval);
    this.interval = initInterval;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long next() {
    if (startTime < 0) {
      throw new IllegalArgumentException("Please set start time at first.");
    }

    long lost = System.currentTimeMillis() - startTime;
    if (lost >= 0) {
      long left = interval * 1000 - lost;
      return left > 0 ? left : 0;
    } else {
      throw new IllegalArgumentException(String.format(
          "FixedBackOffStrategy get backoff time error, start time (%s) is bigger than current time (%s)",
          startTime, lost + startTime));
    }
  }

  public void reset() {

  }
}
