package com.aliyun.odps.udf;

import java.util.Map;
import java.util.Set;

public interface DynamicPtWriteCommitMessage extends CommitMessage {
  Set<Map<String, String>> getDynamicPtWriteInfo();
}
