package com.aliyun.odps.commons.util.backoff;

/**
 * Created by zhenhong.gzh on 17/6/19.
 */

public abstract class BackOffStrategy {
  protected long initInterval;

  /**
   * 失败回退策略， 决定失败到下次重试的等待时间
   *
   * @param initInterval 初始等待时间 (unit: second)
   */
  public BackOffStrategy(long initInterval) {
    if (initInterval < 1) {
      throw new IllegalArgumentException("interval must >= 1");
    }
    this.initInterval = initInterval;
  }

  /**
   * 计算并返回失败到下次重试的等待时间
   *
   * @return 等待时间 (unit: millisecond)
   */
  public abstract long next();

  /**
   * 重置回退策略，将等待时间置为初始间隔时间
   *
   *
   */
  public abstract void reset();
}
