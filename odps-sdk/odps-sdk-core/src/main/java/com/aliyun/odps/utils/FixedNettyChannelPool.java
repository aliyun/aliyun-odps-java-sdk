package com.aliyun.odps.utils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

public class FixedNettyChannelPool {

  private static final Logger logger = LoggerFactory.getLogger(FixedNettyChannelPool.class);

  private final Semaphore availableChannels;
  private final ChannelFactory channelFactory;
  private static final int DEFAULT_RETRY_TIMES = 3;
  private final Map<Channel, Integer> channels = new ConcurrentHashMap<>();
  private int retryTimes = DEFAULT_RETRY_TIMES;
  private final boolean noLimit;

  public FixedNettyChannelPool(int maxChannels, ChannelFactory channelFactory) {
    noLimit = maxChannels <= 0;
    this.availableChannels = new Semaphore(maxChannels, true);
    this.channelFactory = channelFactory;
  }

  public void setRetryTimes(int retryTimes) {
    this.retryTimes = retryTimes;
  }

  public Channel acquire(long timeouts, TimeUnit timeUnit)
      throws InterruptedException, IOException {
    if (noLimit) {
      return acquireChannel();
    }
    if (availableChannels.tryAcquire(timeouts, timeUnit)) {
      try {
        return acquireChannel();
      } catch (Throwable e) {
        availableChannels.release();
        throw e;
      }
    } else {
      throw new IOException("Failed to acquire an active channel with specified timeout");
    }
  }

  public Channel acquire() throws InterruptedException, IOException {
    if (noLimit) {
      return acquireChannel();
    }
    availableChannels.acquire(); // 阻塞直到有可用通道
    try {
      return acquireChannel();
    } catch (Throwable e) {
      availableChannels.release();
      throw e;
    }
  }

  private Channel acquireChannel() throws IOException {
    Channel newChannel = createChannel();
    if (newChannel != null && newChannel.isActive()) {
      channels.put(newChannel, 1);
      return newChannel;
    } else {
      throw new IOException("Failed to create an active channel");
    }
  }

  private Channel createChannel() {
    for (int retry = 0; retry < retryTimes; retry++) {
      try {
        Channel channel = channelFactory.create();
        if (channel != null) {
          return channel;
        }
      } catch (Throwable e) {
        logger.warn("Attempt {} to create channel failed: {}", retry + 1, e.getMessage());
      }
    }
    return null;
  }

  public void release(Channel channel) {
    if (!noLimit && (channels.remove(channel) != null)) {
        availableChannels.release(); // 释放许可，让其他线程能获取
    }
  }

  public int getAcquiredChannelCount() {
    // 返回已获取但未释放的许可数，即当前被占用的通道数
    return availableChannels.getQueueLength();
  }

  public interface ChannelFactory {

    Channel create() throws Exception;
  }
}
