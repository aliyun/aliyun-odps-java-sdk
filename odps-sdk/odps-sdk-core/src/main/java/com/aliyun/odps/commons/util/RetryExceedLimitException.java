package com.aliyun.odps.commons.util;

/**
 * 这个异常将会在 {@link RetryStrategy} 重试超过上限时抛出，并在 msg 中携带重试的次数
 *
 * Created by onesuper on 16/1/9.
 */
public final class RetryExceedLimitException extends Exception {

  public RetryExceedLimitException(int failedAttemps, Throwable cause) {
    super("After " + failedAttemps + " failed attemps", cause);
  }
}
