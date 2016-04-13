package com.aliyun.odps.commons.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by onesuper(yichao.cheng@alibaba-inc.com) on 16/1/8.
 */
public class RetryStrategyTest {

  static int trouble;
  static void troubleMaker(int troubles) throws RuntimeException {
    if (trouble++ != troubles) {
      throw new RuntimeException("I have trouble!!! " + trouble, new RuntimeException("cauze I am cool"));
    }
  }

  @Test
  public void testTroubleMaker() throws Exception {
    trouble = 0;
    while (true) {
      try {
        troubleMaker(5);
      } catch (RuntimeException e) {
        continue;
      }
      break;
    }
    Assert.assertEquals(6, trouble);
  }

  /**
   * 预期：没有错误，不进行任何重试
   *
   * @throws Exception
   */
  @Test
  public void testRetryStrategyConstantNoTrouble() throws Exception {
    RetryStrategy retry = new RetryStrategy(10, 1, RetryStrategy.BackoffStrategy.CONSTANT_BACKOFF);
    while (true) {
      try {
        break;
      } catch (RuntimeException e) {
        retry.onFailure(e);
      }
    }
    Assert.assertEquals(0, retry.getAttempts());
    Assert.assertEquals(0, retry.getTotalBackupTime());
  }

  /**
   * 预期：制造一个会失败 5 次的函数，重试 5 次成功
   *
   * @throws Exception
   */
  @Test
  public void testRetryStrategyConstantHold() throws Exception {
    RetryStrategy retry = new RetryStrategy(10, 1, RetryStrategy.BackoffStrategy.CONSTANT_BACKOFF);
    trouble = 0;
    while (true) {
      try {
        troubleMaker(5);
        break;
      } catch (RuntimeException e) {
        retry.onFailure(e);
      }
    }
    Assert.assertEquals(5, retry.getAttempts());
    Assert.assertEquals(5, retry.getTotalBackupTime());
    Assert.assertEquals(6, trouble);
  }

  /**
   * 预期：制造一个会失败 5 次的函数，在重试 4 次后 fail 掉
   *
   * @throws Exception
   */
  @Test
  public void testRetryStrategyConstantExceedLimit() {
    RetryStrategy retry = new RetryStrategy(3, 1, RetryStrategy.BackoffStrategy.CONSTANT_BACKOFF);
    trouble = 0;
    try {
      while (true) {
        try {
          troubleMaker(5);
          break;
        } catch (RuntimeException e) {
          retry.onFailure(e);
        }
      }
      Assert.assertFalse(true);
    } catch (RetryExceedLimitException err) {
      Assert.assertTrue(err.getMessage().indexOf("After 4 failed attemps") != -1);
      Assert.assertTrue(err.getCause().getMessage().indexOf("I have trouble!!! ") != -1);
      Assert.assertTrue(err.getCause().getCause().getMessage().indexOf("cauze I am cool") != -1);
      Assert.assertEquals(4, trouble);
    }
  }

  /**
   * 预期：制造一个会失败 4 次的函数，重试 4 次成功
   *
   * @throws Exception
   */
  @Test
  public void testRetryStrategyLinearHold() throws Exception {
    RetryStrategy retry = new RetryStrategy(10, 1, RetryStrategy.BackoffStrategy.LINEAR_BACKOFF);
    trouble = 0;
    while (true) {
      try {
        troubleMaker(4);
        break;
      } catch (RuntimeException e) {
        retry.onFailure(e);
      }
    }
    Assert.assertEquals(4, retry.getAttempts());
    Assert.assertEquals(1+2+3+4, retry.getTotalBackupTime());
    Assert.assertEquals(5, trouble);
  }

  /**
   * 预期：制造一个会失败 3 次的函数，重试 3 次成功
   *
   * @throws Exception
   */
  @Test
  public void testRetryStrategyExponentialHold() throws Exception {
    RetryStrategy retry = new RetryStrategy(10, 1, RetryStrategy.BackoffStrategy.EXPONENTIAL_BACKOFF);
    trouble = 0;
    while (true) {
      try {
        troubleMaker(3);
        break;
      } catch (RuntimeException e) {
        retry.onFailure(e);
      }
    }
    Assert.assertEquals(3, retry.getAttempts());
    Assert.assertEquals(1+2+4, retry.getTotalBackupTime());
    Assert.assertEquals(4, trouble);
  }
}
