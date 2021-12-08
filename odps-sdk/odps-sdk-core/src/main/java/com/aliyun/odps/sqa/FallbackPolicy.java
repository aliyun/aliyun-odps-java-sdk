package com.aliyun.odps.sqa;

/**
 * Created by dongxiao on 2020/4/21.
 */
public class FallbackPolicy {
  // fallback when resource not enough exception happened
  private boolean fallback4ResourceNotEnough = true;
  // fallback when unsupported feature
  private boolean fallback4UnsupportedFeature = true;
  // fallback when query running timeout
  private boolean fallback4RunningTimeout = true;
  // fallback during service upgrading
  private boolean fallback4Upgrading = true;
  // fallback when unknown error happened
  private boolean fallback4UnknownError = true;
  // fallback when attach session failed
  private boolean fallback4AttachError = true;

  FallbackPolicy() {
  }

  public static FallbackPolicy alwaysFallbackPolicy() {
    FallbackPolicy policy = new FallbackPolicy();
    return policy.fallback4ResourceNotEnough(true)
        .fallback4UnsupportedFeature(true)
        .fallback4RunningTimeout(true)
        .fallback4Upgrading(true)
        .fallback4UnknownError(true)
        .fallback4AttachError(true);
  }

  public static FallbackPolicy alwaysFallbackExceptAttachPolicy() {
    FallbackPolicy policy = new FallbackPolicy();
    return policy.fallback4ResourceNotEnough(true)
        .fallback4UnsupportedFeature(true)
        .fallback4RunningTimeout(true)
        .fallback4Upgrading(true)
        .fallback4UnknownError(true)
        .fallback4AttachError(false);
  }

  public static FallbackPolicy nonFallbackPolicy() {
    FallbackPolicy policy = new FallbackPolicy();
    return policy.fallback4ResourceNotEnough(false)
        .fallback4UnsupportedFeature(false)
        .fallback4RunningTimeout(false)
        .fallback4Upgrading(false)
        .fallback4UnknownError(false)
        .fallback4AttachError(false);
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

  public FallbackPolicy fallback4AttachError(boolean enable) {
    fallback4AttachError = enable;
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

  public boolean isFallback4AttachError() {
    return fallback4AttachError;
  }

  public boolean isAlwaysFallBack() {
    return fallback4ResourceNotEnough && fallback4UnsupportedFeature && fallback4RunningTimeout
        && fallback4Upgrading && fallback4UnknownError && fallback4AttachError;
  }
}
