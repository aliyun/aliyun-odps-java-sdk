package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;

public class PartitionRecord extends ArrayRecord {

    private String partition;

    public String getPartition() {
        return partition;
    }

    public void setPartition(PartitionSpec partition) {
        this.partition = partition.toString().replaceAll("'", "");
    }

    public PartitionRecord(Column[] columns) {
        super(columns);
    }

    public PartitionRecord(Column[] columns, boolean strictTypeValidation) {
        super(columns, strictTypeValidation);
    }

    public PartitionRecord(Column[] columns, boolean strictTypeValidation, Long fieldMaxSize) {
        super(columns, strictTypeValidation, fieldMaxSize);
    }

    public PartitionRecord(Column[] columns, boolean strictTypeValidation, Long fieldMaxSize,
                           boolean caseSensitive) {
        super(columns, strictTypeValidation, fieldMaxSize, caseSensitive);
    }

    public PartitionRecord(Column[] columns, Object[] values, boolean strictTypeValidation,
                           boolean caseSensitive) {
        super(columns, values, strictTypeValidation, caseSensitive);
    }

    public PartitionRecord(Column[] columns, Object[] values) {
        super(columns, values);
    }

    public PartitionRecord(Column[] columns, Object[] values, boolean strictTypeValidation) {
        super(columns, values, strictTypeValidation);
    }

    public PartitionRecord(TableSchema schema) {
        super(schema);
    }

    public PartitionRecord(TableSchema schema, boolean strictTypeValidation) {
        super(schema, strictTypeValidation);
    }
}
