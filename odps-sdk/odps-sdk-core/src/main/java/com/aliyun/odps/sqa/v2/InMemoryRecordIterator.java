package com.aliyun.odps.sqa.v2;

import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.data.Record;

/**
 * class: OfflineRecordSetIterator It is used in getOfflineResultSet It passes a List to fake a
 * reader The record count in List is limited, so there is no memory problem
 */
public class InMemoryRecordIterator implements Iterator<Record> {

    private int cursor = 0;
    private List<Record> buffer;

    public InMemoryRecordIterator(List<Record> buffer) {
        this.buffer = buffer;
    }

    @Override
    public boolean hasNext() {
        if (buffer == null) {
            return false;
        }
        return cursor < buffer.size();
    }

    @Override
    public Record next() {
        return buffer.get(cursor++);
    }
}
