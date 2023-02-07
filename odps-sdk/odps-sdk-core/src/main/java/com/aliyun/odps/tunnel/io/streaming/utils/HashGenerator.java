package com.aliyun.odps.tunnel.io.streaming.utils;

public class HashGenerator {
  // TODO 替换为与SQL一样的hash函数
  public static int sqlHashForHashClusteredTable(byte[] key) {
    int hash = key.length;
    for (byte b : key) {
      hash += b;
    }
    return hash;
  }

  public static int getHashBucketResult(int finalHash, int bucketNum) {
    return (finalHash % bucketNum);
  }
}
