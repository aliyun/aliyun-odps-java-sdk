package com.aliyun.odps.udf;

public interface CommitMessage {
  byte[] serialize();

  void deserialize(byte[] infos);
}
