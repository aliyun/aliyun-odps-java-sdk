package com.aliyun.odps.sqa;

/**
 * Created by dongxiao on 2020/4/21.
 */
public class FallbackPolicy {
  // fallback when resource not enough exception happened
  private boolean fallback4ResourceNotEnough = false;
  // fallback when unsupported feature
  private boolean fallback4UnsupportedFeature = false;
  // fallback when query running timeout
  private boolean fallback4RunningTimeout = false;
  // fallback during service upgrading
  private boolean fallback4Upgrading = false;
  // fallback when unknown error happened
  private boolean fallback4UnknownError = false;

  FallbackPolicy() {
  }

  public static FallbackPolicy alwaysFallbackPolicy() {
    FallbackPolicy policy = new FallbackPolicy();
    return policy.fallback4ResourceNotEnough(true)
        .fallback4UnsupportedFeature(true)
        .fallback4RunningTimeout(true)
        .fallback4Upgrading(true)
        .fallback4UnknownError(true);
  }

  public static FallbackPolicy nonFallbackPolicy() {
    return new FallbackPolicy();
  }

  public FallbackPolicy fallback4ResourceNotEnough(boolean enable) {
    fallback4ResourceNotEnough = enable;
    return this;
  }

  public FallbackPolicy fallback4UnsupportedFeature(boolean enable) {
    fallback4UnsupportedFeature = enable;
    return this;
  }

  public FallbackPolicy fallback4RunningTimeout(boolean enable) {
    fallback4RunningTimeout = enable;
    return this;
  }

  public FallbackPolicy fallback4Upgrading(boolean enable) {
    fallback4Upgrading = enable;
    return this;
  }

  public FallbackPolicy fallback4UnknownError(boolean enable) {
    fallback4UnknownError = enable;
    return this;
  }

  public boolean isFallback4ResourceNotEnough() {
    return fallback4ResourceNotEnough;
  }

  public boolean isFallback4UnsupportedFeature() {
    return fallback4UnsupportedFeature;
  }

  public boolean isFallback4RunningTimeout() {
    return fallback4RunningTimeout;
  }

  public boolean isFallback4Upgrading() {
    return fallback4Upgrading;
  }

  public boolean isFallback4UnknownError() {
    return fallback4UnknownError;
  }

  public boolean isAlwaysFallBack() {
    return fallback4ResourceNotEnough && fallback4UnsupportedFeature && fallback4RunningTimeout
        && fallback4Upgrading && fallback4UnknownError;
  }
}
