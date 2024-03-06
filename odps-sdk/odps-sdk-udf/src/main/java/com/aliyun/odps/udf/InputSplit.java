package com.aliyun.odps.udf;

public interface InputSplit {
    byte[] serialize();

    void deserialize(byte[] infos);

    default int getEstimateRows() { return 0; }

    default long getEstimateBytes() { return 0; }
}
