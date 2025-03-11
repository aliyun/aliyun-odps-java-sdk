package com.aliyun.odps.tunnel;

import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TunnelMetrics {

  private long networkWallCost;
  private long clientProcessCost;
  private long tunnelProcessCost;
  private long storageCost;
  private long serverTotalCost;
  private long serverIoCost;
  private long rateLimitCost;


  public TunnelMetrics() {
  }

  TunnelMetrics(long networkWallCost, long clientProcessCost,
                long tunnelProcessCost, long storageCost, long serverTotalCost,
                long serverIoCost, long rateLimitCost) {
    this.networkWallCost = networkWallCost;
    this.clientProcessCost = clientProcessCost;
    this.tunnelProcessCost = tunnelProcessCost;
    this.storageCost = storageCost;
    this.serverTotalCost = serverTotalCost;
    this.serverIoCost = serverIoCost;
    this.rateLimitCost = rateLimitCost;
  }

  public static TunnelMetrics parse(String metricsString, long localWallTime,
                                    long networkWallTime) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      Map<String, Long>
          costMap =
          objectMapper.readValue(metricsString, new TypeReference<Map<String, Long>>() {
          });
      // The unit returned by the server is microseconds
      long storageCost = costMap.getOrDefault("PanguIOCost", 0L) / 1000;
      long serverIoCost = costMap.getOrDefault("ServerIOCost", 0L) / 1000;
      long serverTotalCost = costMap.getOrDefault("ServerTotalCost", 0L) / 1000;
      long rateLimitCost = costMap.getOrDefault("RateLimitCost", 0L) / 1000;

      return new TunnelMetrics(networkWallTime,
                               localWallTime - networkWallTime,
                               serverTotalCost - serverIoCost - storageCost - rateLimitCost,
                               storageCost,
                               serverTotalCost,
                               serverIoCost,
                               rateLimitCost
      );

    } catch (Exception ignored) {
      // parse failed do not break the process
      return new TunnelMetrics();
    }
  }

  public void add(TunnelMetrics other) {
    this.networkWallCost += other.networkWallCost;
    this.clientProcessCost += other.clientProcessCost;
    this.tunnelProcessCost += other.tunnelProcessCost;
    this.storageCost += other.storageCost;
    this.serverTotalCost += other.serverTotalCost;
    this.serverIoCost += other.serverIoCost;
    this.rateLimitCost += other.rateLimitCost;
  }

  @Override
  public String toString() {
    return "TunnelMetrics{" +
        "networkWallCost=" + networkWallCost +
        ", clientProcessCost=" + clientProcessCost +
        ", tunnelProcessCost=" + tunnelProcessCost +
        ", storageCost=" + storageCost +
        ", serverTotalCost=" + serverTotalCost +
        ", serverIoCost=" + serverIoCost +
        ", rateLimitCost=" + rateLimitCost +
        '}';
  }
}
