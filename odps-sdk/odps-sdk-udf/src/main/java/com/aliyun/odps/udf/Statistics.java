package com.aliyun.odps.udf;

public interface Statistics {
    void reset();
    int getBytesRead();
    int getBytesWritten();
}
