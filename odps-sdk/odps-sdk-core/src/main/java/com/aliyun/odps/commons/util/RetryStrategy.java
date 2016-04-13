package com.aliyun.odps.commons.util;

/**
 * 失败重试策略：辅助用户对某段代码逻辑进行重试处理
 *
 * <b>Example</b>
 *
 * <pre>
 * // 创建一个重试策略，它将忽略 5 次异常，每遇到一次错误，分别回避 1s、2s、4s、8s、16s
 * RetryStrategy retry = new RetryStrategy(5, 1, BackoffStrategy.EXPONENTIAL_BACKOFF);
 *
 * try {
 *   while (true) {
 *     try {
 *       // ...
 *       // 用户自定义的需要重试的逻辑，可能抛出异常 e1, e2
 *       // ..
 *       break;
 *     } catch (e1) {
 *       retry.onFailure(e1);  // 对 e1 进行重试处理
 *     } catch (e2) {
 *       retry.onFailure(e1);  // 对 e2 进行重试处理
 *     }
 *   }
 * } catch (RetryExceedLimitException ex) {
 *    // 对于超过重试次数的情况进行处理
 * }
 * </pre>
 *
 * Created by onesuper(yichao.cheng@alibaba-inc.com) on 16/1/8.
 */
public class RetryStrategy {
  /**
   * 失败回避策略
   * EXPONENTIAL_BACKOFF：指数回避策略：失败到下次重试的等待间隔按指数递增
   * CONSTANT_BACKOFF：常数回避策略：失败到下次重试的等待间隔是一个定值
   * LINEAR_BACKOFF：常数回避策略：失败到下次重试的等待间隔是一个常数递增的值（每次递增一个默认值）
   */
  public enum BackoffStrategy {
    EXPONENTIAL_BACKOFF,
    LINEAR_BACKOFF,
    CONSTANT_BACKOFF,
  }

  /**
   * 默认失败重试次数：10 次
   */
  final static private int DEFAULT_ATTEMPTS = 10;

  /**
   * 默认的失败到下次重试的等待间隔，1 秒
   */
  final static private int DEFAULT_BACKOFF_INTERVAL = 1;

  /**
   * 失败回避策略
   */
  private BackoffStrategy strategy;

  /**
   * 失败重试次数，当它的值等于 limit，就不再重试，抛出异常
   */
  private int attempts;

  private int limit;

  /**
   * 发生失败到到下次重试的时间间隔，秒
   *
   * 对于指数回避策略，这个值会以指数增长。
   * 对于常数回避策略，这个值将保持不变。
   */
  private int interval;

  private int initialInterval;

  /**
   * 总共消耗在回避上的时间，秒
   */
  private int totalBackupTime;

  /**
   * 构造重试策略
   *
   * @param limit 尝试次数上限
   * @param interval 初始发生失败到到下次重试的时间间隔
   * @param strategy 回避策略
   */
  public RetryStrategy(int limit, int interval, BackoffStrategy strategy) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must >= 1");
    }
    if (interval < 1) {
      throw new IllegalArgumentException("interval must >= 1");
    }
    this.limit = limit;
    this.initialInterval = interval;
    this.interval = interval;
    this.strategy = strategy;
    this.totalBackupTime = 0;
    this.attempts = 0;
  }

  /**
   * 构造重试策略
   *
   * @param attempts 尝试次数
   * @param interval 发生失败到到下次重试的时间间隔
   */
  public RetryStrategy(int attempts, int interval) {
    this(attempts, interval, BackoffStrategy.CONSTANT_BACKOFF);
  }

  /**
   * 构造重试策略
   *
   * @param attempts 尝试次数
   */
  public RetryStrategy(int attempts) {
    this(attempts, DEFAULT_BACKOFF_INTERVAL, BackoffStrategy.CONSTANT_BACKOFF);
  }

  /**
   * 构造重试策略
   */
  public RetryStrategy() {
    this(DEFAULT_ATTEMPTS, DEFAULT_BACKOFF_INTERVAL, BackoffStrategy.CONSTANT_BACKOFF);
  }

  /**
   * 该方法会忽略一定次数的失败，并策略性地进行回避，然后进入后续的逻辑（重试）
   * 直到达到重试上限时会抛出异常。
   *
   * @param err 用户 catch 到的异常
   */
  public void onFailure(Exception err) throws RetryExceedLimitException {
    if (attempts++ >= limit) {
      throw new RetryExceedLimitException(attempts, err);
    }

    try {
      Thread.sleep(interval * 1000);
      totalBackupTime += interval;
      if (strategy.equals(BackoffStrategy.EXPONENTIAL_BACKOFF)) {
        interval *= 2;
      }
      if (strategy.equals(BackoffStrategy.LINEAR_BACKOFF)) {
        interval += initialInterval;
      }
    } catch (InterruptedException ignore) {
    }
  }

  /**
   * 获取到目前为止尝试了的次数
   *
   * @return 重试次数
   */
  public int getAttempts() {
    return attempts;
  }

  /**
   * 获取总共消耗在回避上的时间，秒
   */
  public int getTotalBackupTime() {
    return totalBackupTime;
  }
}
