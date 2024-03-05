package com.aliyun.odps.commons.util;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.util.backoff.BackOffStrategy;
import com.aliyun.odps.commons.util.backoff.ConstantBackOffStrategy;
import com.aliyun.odps.commons.util.backoff.ExponentialBackOffStrategy;
import com.aliyun.odps.commons.util.backoff.LinearBackOffStrategy;
import com.aliyun.odps.rest.RestClient.RetryLogger;

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
 * <p>
 * Created by onesuper(yichao.cheng@alibaba-inc.com) on 16/1/8.
 */
public class RetryStrategy {

  /**
   * 失败回避策略
   * EXPONENTIAL_BACKOFF：指数回避策略：失败到下次重试的等待间隔按指数递增
   * CONSTANT_BACKOFF：常数回避策略：失败到下次重试的等待间隔是一个定值
   * LINEAR_BACKOFF：常数回避策略：失败到下次重试的等待间隔是一个常数递增的值（每次递增一个默认值）
   */
  @Deprecated
  public enum BackoffStrategy {
    EXPONENTIAL_BACKOFF,
    LINEAR_BACKOFF,
    CONSTANT_BACKOFF,
  }

  private BackOffStrategy transform(BackoffStrategy strategy, long initInterval) {
    switch (strategy) {
      case CONSTANT_BACKOFF:
        return new ConstantBackOffStrategy(initInterval);
      case LINEAR_BACKOFF:
        return new LinearBackOffStrategy(initInterval);
      case EXPONENTIAL_BACKOFF:
        return new ExponentialBackOffStrategy(initInterval);
      default:
        throw new IllegalArgumentException("Invalid retry strategy: " + strategy.name());
    }
  }

  final static private Logger LOG = Logger.getLogger(RetryStrategy.class.getName());

  /**
   * 默认失败重试次数：10 次
   */
  final static private int DEFAULT_ATTEMPTS = 10;

  /**
   * retryAfter 的最大值 120s
   */
  final static private long MAX_RETRY_AFTER = 120;
  /**
   * 默认的失败到下次重试的等待间隔，1 秒
   */
  final static private int DEFAULT_BACKOFF_INTERVAL = 1;

  /**
   * 失败回避策略
   */
  protected BackOffStrategy strategy;

  /**
   * 失败重试次数，当它的值等于 limit，就不再重试，抛出异常
   */
  protected int attempts;

  protected int limit;

  /**
   * 总共消耗在回避上的时间，秒
   */
  protected int totalBackoffTime;

  private void init(int limit, BackOffStrategy strategy) {
    if (limit < 0) {
      throw new IllegalArgumentException("Retry limit must >= 0");
    }

    this.limit = limit;
    this.strategy = strategy;
    this.totalBackoffTime = 0;
    this.attempts = 0;
  }

  /**
   * 构造重试策略
   *
   * @param limit    尝试次数上限
   * @param strategy 回避策略
   */
  public RetryStrategy(int limit, BackOffStrategy strategy) {
    init(limit, strategy);
  }

  /**
   * 构造重试策略
   *
   * @param limit    尝试次数上限
   * @param interval 初始发生失败到到下次重试的时间间隔
   * @param strategy 回避策略
   */
  public RetryStrategy(int limit, int interval, BackoffStrategy strategy) {
    init(limit, transform(strategy, interval));
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
   * 重置重试策略，重试次数和间隔回到初始值。
   */
  public void reset() {
    attempts = 0;
    strategy.reset();
  }

  protected boolean needRetry(Exception e) {
    return true;
  }


  public void onFailure(Exception err) throws RetryExceedLimitException, InterruptedException {
    onFailure(err, null);
  }

  /**
   * 该方法会忽略一定次数的失败，并策略性地进行回避，然后进入后续的逻辑（重试）
   * 直到达到重试上限时会抛出异常。
   *
   * @param err    用户 catch 到的异常
   * @param logger 错误日志
   */

  public void onFailure(Exception err, RetryLogger logger)
      throws RetryExceedLimitException, InterruptedException {
    if (!needRetry(err)) {
      throw new RetryExceedLimitException(0, err);
    }

    if (attempts++ >= limit) {
      throw new RetryExceedLimitException(attempts, err);
    }

    long millis = strategy.next();
    if (err instanceof OdpsException) {
      OdpsException ret = (OdpsException) (err);
      Long retryAfter = ret.getRetryAfter();
      if (retryAfter != null && retryAfter >= 0) {
        retryAfter = Math.min(retryAfter, MAX_RETRY_AFTER);
        millis = retryAfter * 1000;
      }
    }
    if (logger != null) {
      logger.onRetryLog(err, attempts, millis / 1000);
    } else if (LOG.isLoggable(Level.FINE)) {
      LOG.fine(String.format("Start to retry, retryCount: %d, will retry in %d seconds.",
                             attempts, millis / 1000));
    }

    Thread.sleep(millis);
    totalBackoffTime += (millis / 1000);
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
    return totalBackoffTime;
  }
}
