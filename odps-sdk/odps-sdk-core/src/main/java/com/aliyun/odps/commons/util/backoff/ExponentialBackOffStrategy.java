package com.aliyun.odps.commons.util.backoff;

/**
 * Created by zhenhong.gzh on 17/6/19.
 */
public class ExponentialBackOffStrategy extends BackOffStrategy {
  private static final int DEFAULT_TIMES = 2;

  private long interval;
  private boolean first = true;

  /**
   * 指数回避策略：发生失败到下次重试请求的等待间隔按指数递增
   *
   * @param initInterval 初始间隔
   */
  public ExponentialBackOffStrategy(long initInterval) {
    super(initInterval);
    this.interval = initInterval;
  }

  public long next() {
    int times = DEFAULT_TIMES;
    if (first) {
      times = 1;
      first = false;
    }

    interval *= times;
    return interval * 1000;
  }

  public void reset() {
    interval = initInterval;
    first = true;
  }
}
