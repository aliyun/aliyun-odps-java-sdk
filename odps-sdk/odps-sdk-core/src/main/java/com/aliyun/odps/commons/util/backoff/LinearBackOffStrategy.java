package com.aliyun.odps.commons.util.backoff;

/**
 * Created by zhenhong.gzh on 17/6/19.
 */
public class LinearBackOffStrategy extends BackOffStrategy {
  private long interval;


  /**
   * 常数回避策略：失败到下次重试的等待间隔是一个常数递增的值（每次递增一个初始值）
   *
   * @param initInterval 初始间隔
   */
  public LinearBackOffStrategy(long initInterval) {
    super(initInterval);
    this.interval = 0;
  }

  public long next() {
    interval += initInterval;
    return interval * 1000;
  }

  public void reset() {
    interval = initInterval;
  }
}
