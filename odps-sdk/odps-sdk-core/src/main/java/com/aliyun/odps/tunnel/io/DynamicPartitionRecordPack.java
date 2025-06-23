package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.impl.PartitionRecord;
import com.aliyun.odps.tunnel.impl.StreamUploadSessionImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DynamicPartitionRecordPack  implements TableTunnel.StreamRecordPack{
    private Map<String, StreamRecordPackImpl> mBufferMap = new HashMap<>();
    private StreamUploadSessionImpl session;
    private CompressOption compressOption;

    public DynamicPartitionRecordPack(StreamUploadSessionImpl session, CompressOption option) {
        this.session = session;
        this.compressOption = option;
    }

    @Override
    public void append(Record record) throws IOException {
        PartitionRecord r = (PartitionRecord) record;
        String partition = r.getPartition();
        StreamRecordPackImpl pack = mBufferMap.get(partition);
        if (pack == null) {
            pack = new StreamRecordPackImpl(session, compressOption);
            mBufferMap.put(partition, pack);
        }
        pack.append(record);
    }

    @Override
    public long getRecordCount() {
        long total = mBufferMap.values().stream()
            .reduce(0L, (sum, pack) -> sum + pack.getRecordCount(), Long::sum);
        return total;
    }

    @Override
    public long getDataSize() {
        long total = mBufferMap.values().stream()
                .reduce(0L, (sum, pack) -> sum + pack.getDataSize(), Long::sum);
        return total;
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
        for (StreamRecordPackImpl pack : mBufferMap.values()) {
            if (pack.getRecordCount() > 0) {
                if (!first) {
                    idBuilder.append(",");
                }
                TableTunnel.FlushResult result = pack.flush(flushOption);
                idBuilder.append(result.getTraceId());
                flushSize += result.getFlushSize();
                recordCount += result.getRecordCount();
                first = false;
            }
        }
        return new FlushResultImpl(idBuilder.toString(), flushSize, recordCount);
    }

    @Override
    public void reset() throws IOException {
        for (StreamRecordPackImpl pack : mBufferMap.values()) {
            pack.reset();
        }
    }
}
