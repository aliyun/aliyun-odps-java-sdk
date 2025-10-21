package com.aliyun.odps.udf;

public interface ExtendedStatistics {
    void reset();
    long getBytesRead();
    long getBytesWritten();
    long getRowsRead();
    long getRowsWritten();
}
