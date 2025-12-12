package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.impl.StreamUploadSessionImpl;

import java.util.Map;

class FlushResultImpl implements TableTunnel.FlushResult {
    private String traceId;
    private long batchId = 0;
    private long flushSize;
    private long recordCount;

    public FlushResultImpl(StreamUploadSessionImpl.WriteResult result, long flushSize, long recordCount) {
        this.traceId = result.requestId;
        this.batchId = result.batchId;
        this.flushSize = flushSize;
        this.recordCount = recordCount;
    }

    @Override
    public String getTraceId() {
        return traceId;
    }

    @Override
    public long getBatchId() {
        return batchId;
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
