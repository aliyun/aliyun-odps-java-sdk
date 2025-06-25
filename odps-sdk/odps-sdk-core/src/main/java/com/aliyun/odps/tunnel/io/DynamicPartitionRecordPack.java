package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.impl.PartitionRecord;
import com.aliyun.odps.tunnel.impl.StreamUploadSessionImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DynamicPartitionRecordPack implements TableTunnel.StreamRecordPack {

    private final Map<String, StreamRecordPackImpl> mBufferMap = new HashMap<>();
    private final StreamUploadSessionImpl session;
    private final CompressOption compressOption;

    public DynamicPartitionRecordPack(StreamUploadSessionImpl session, CompressOption option) {
        this.session = session;
        this.compressOption = option;
    }

    @Override
    public void append(Record record) throws IOException {
        PartitionRecord r = (PartitionRecord) record;
        append(r, r.getPartition());
    }

    public void append(Record record, String partition) throws IOException {
        if (null == partition) {
            throw new IOException(new TunnelException("partition is required for PartitionRecord"));
        }
        StreamRecordPackImpl pack = mBufferMap.get(partition);
        if (pack == null) {
            pack = new StreamRecordPackImpl(session, compressOption);
            mBufferMap.put(partition, pack);
        }
        pack.append(record);
    }

    @Override
    public long getRecordCount() {
        return mBufferMap.values().stream()
            .reduce(0L, (sum, pack) -> sum + pack.getRecordCount(), Long::sum);
    }

    @Override
    public long getDataSize() {
        return mBufferMap.values().stream()
            .reduce(0L, (sum, pack) -> sum + pack.getDataSize(), Long::sum);
    }

    @Override
    public String flush() throws IOException {
        return flush(new TableTunnel.FlushOption()).getTraceId();
    }

    @Override
    public TableTunnel.FlushResult flush(TableTunnel.FlushOption flushOption) throws IOException {
        long flushSize = 0;
        long recordCount = 0;
        StringBuilder idBuilder = new StringBuilder();
        boolean first = true;
        long batchId = Long.MIN_VALUE;
        for (Map.Entry<String, StreamRecordPackImpl> entry : mBufferMap.entrySet()) {
            String partition = entry.getKey();
            StreamRecordPackImpl pack = entry.getValue();
            if (pack.getRecordCount() > 0) {
                if (!first) {
                    idBuilder.append(",");
                }
                TableTunnel.FlushResult result = pack.flush(flushOption, partition);
                idBuilder.append(result.getTraceId());
                flushSize += result.getFlushSize();
                recordCount += result.getRecordCount();
                batchId = Long.max(batchId, result.getBatchId());
                first = false;
            }
        }
        StreamUploadSessionImpl.WriteResult writeResult = new StreamUploadSessionImpl.WriteResult();
        writeResult.requestId = idBuilder.toString();
        writeResult.batchId = batchId;
        return new FlushResultImpl(writeResult, flushSize, recordCount);
    }

    @Override
    public void reset() throws IOException {
        for (StreamRecordPackImpl pack : mBufferMap.values()) {
            pack.reset();
        }
    }
}
