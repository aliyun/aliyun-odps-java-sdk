package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.tunnel.TableTunnel;

class FlushResultImpl implements TableTunnel.FlushResult {
    private String traceId;
    private long flushSize;
    private long recordCount;

    public FlushResultImpl(String traceId, long flushSize, long recordCount) {
        this.traceId = traceId;
        this.flushSize = flushSize;
        this.recordCount = recordCount;
    }

    @Override
    public String getTraceId() {
        return traceId;
    }

    @Override
    public long getFlushSize() {
        return flushSize;
    }

    @Override
    public long getRecordCount() {
        return recordCount;
    }
}
