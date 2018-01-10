package com.aliyun.odps.commons.util.backoff;

/**
 * Created by zhenhong.gzh on 17/6/19.
 */
public class ConstantBackOffStrategy extends BackOffStrategy {
  private long interval;

  /**
   * 常数回避策略：发生失败后，到下次重试请求的等待间隔是一个定值
   *
   *
   * @param initInterval 初始间隔
   */
  public ConstantBackOffStrategy(long initInterval) {
    super(initInterval);
    this.interval = initInterval;
  }

  public long next() {
    return interval * 1000;
  }

  public void reset() {

  }
}
