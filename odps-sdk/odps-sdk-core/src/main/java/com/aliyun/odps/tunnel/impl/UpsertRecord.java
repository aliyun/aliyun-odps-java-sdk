package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TunnelConstants;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

class UpsertRecord extends ArrayRecord {
    private Column [] columns;
    ArrayRecord record;
    private HashMap<String, Integer> nameMap = new HashMap<>();

    public UpsertRecord(Column[] columns) {
        this(columns, null);
    }

    public UpsertRecord(Column[] columns, Object[] values) {
        super(columns);
        if (columns.length < 5) {
            throw new IllegalArgumentException("Incomplete schema");
        }
        if (!columns[columns.length - 5].getName().equalsIgnoreCase(TunnelConstants.META_FIELD_VERSION))
        {
            throw new IllegalArgumentException("Invalid schema");
        }
        if (!columns[columns.length - 4].getName().equalsIgnoreCase(TunnelConstants.META_FIELD_APP_VERSION))
        {
            throw new IllegalArgumentException("Invalid schema");
        }
        if (!columns[columns.length - 3].getName().equalsIgnoreCase(TunnelConstants.META_FIELD_OPERATION))
        {
            throw new IllegalArgumentException("Invalid schema");
        }
        if (!columns[columns.length - 2].getName().equalsIgnoreCase(TunnelConstants.META_FIELD_KEY_COLS))
        {
            throw new IllegalArgumentException("Invalid schema");
        }
        if (!columns[columns.length - 1].getName().equalsIgnoreCase(TunnelConstants.META_FIELD_VALUE_COLS))
        {
            throw new IllegalArgumentException("Invalid schema");
        }
        this.columns = new Column[columns.length - 5];
        for (int i  = 0; i < columns.length - 5; ++i) {
            this.columns[i] = columns[i];
        }
        for (int i = 0; i < columns.length; i++) {
            nameMap.put(columns[i].getName(), i);
        }
        if (values == null) {
            record = new ArrayRecord(columns);
        } else {
            record = new ArrayRecord(columns, values);
        }
    }

    void setOperation(byte operation) {
        record.setTinyint(TunnelConstants.META_FIELD_OPERATION, operation);
    }

    void setValueCols(List<Integer> cols) {
        record.setArray(TunnelConstants.META_FIELD_VALUE_COLS, cols);
    }

    ArrayRecord getRecord() {
        return record;
    }

    @Override
    public int getColumnCount() {
        return columns.length;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }

    @Override
    public boolean isNull(int idx) {
        checkIndex(idx);
        return record.isNull(idx);
    }

    @Override
    public boolean isNull(String columnName) {
        return isNull(getColumnIndex(columnName));
    }

    @Override
    public void set(int idx, Object value) {
        checkIndex(idx);
        record.set(idx, value);
    }

    @Override
    public Object get(int idx) {
        checkIndex(idx);
        return record.get(idx);
    }

    @Override
    public void set(String columnName, Object value) {
        set(getColumnIndex(columnName), value);
    }

    @Override
    public Object get(String columnName) {
        return get(getColumnIndex(columnName));
    }

    @Override
    public void setBigint(int idx, Long value) {
        checkIndex(idx);
        record.setBigint(idx, value);
    }

    @Override
    public Long getBigint(int idx) {
        checkIndex(idx);
        return record.getBigint(idx);
    }

    @Override
    public void setBigint(String columnName, Long value) {
        setBigint(getColumnIndex(columnName), value);
    }

    @Override
    public Long getBigint(String columnName) {
        return getBigint(getColumnIndex(columnName));
    }

    @Override
    public void setDouble(int idx, Double value) {
        checkIndex(idx);
        record.setDouble(idx, value);
    }

    @Override
    public Double getDouble(int idx) {
        checkIndex(idx);
        return record.getDouble(idx);
    }

    @Override
    public void setDouble(String columnName, Double value) {
        setDouble(getColumnIndex(columnName), value);
    }

    @Override
    public Double getDouble(String columnName) {
        return getDouble(getColumnIndex(columnName));
    }

    @Override
    public void setBoolean(int idx, Boolean value) {
        checkIndex(idx);
        record.setBoolean(idx, value);
    }

    @Override
    public Boolean getBoolean(int idx) {
        checkIndex(idx);
        return record.getBoolean(idx);
    }

    @Override
    public void setBoolean(String columnName, Boolean value) {
        setBoolean(getColumnIndex(columnName), value);
    }

    @Override
    public Boolean getBoolean(String columnName) {
        return getBoolean(getColumnIndex(columnName));
    }

    @Override
    public void setDatetime(int idx, Date value) {
        checkIndex(idx);
        record.setDatetime(idx, value);
    }

    @Override
    public Date getDatetime(int idx) {
        checkIndex(idx);
        return record.getDatetime(idx);
    }

    @Override
    public void setDatetime(String columnName, Date value) {
        setDatetime(getColumnIndex(columnName), value);
    }

    @Override
    public Date getDatetime(String columnName) {
        return getDatetime(getColumnIndex(columnName));
    }

    @Override
    public void setDecimal(int idx, BigDecimal value) {
        checkIndex(idx);
        record.setDecimal(idx, value);
    }

    @Override
    public BigDecimal getDecimal(int idx) {
        checkIndex(idx);
        return record.getDecimal(idx);
    }

    @Override
    public void setDecimal(String columnName, BigDecimal value) {
        setDecimal(getColumnIndex(columnName), value);
    }

    @Override
    public BigDecimal getDecimal(String columnName) {
        return getDecimal(getColumnIndex(columnName));
    }

    @Override
    public void setString(int idx, String value) {
        checkIndex(idx);
        record.setString(idx, value);
    }

    @Override
    public String getString(int idx) {
        checkIndex(idx);
        return record.getString(idx);
    }

    @Override
    public void setString(String columnName, String value) {
        setString(getColumnIndex(columnName), value);
    }

    @Override
    public String getString(String columnName) {
        return getString(getColumnIndex(columnName));
    }

    @Override
    public void setString(int idx, byte[] value) {
        checkIndex(idx);
        record.setString(idx, value);
    }

    @Override
    public void setString(String columnName, byte[] value) {
        setString(getColumnIndex(columnName), value);
    }

    @Override
    public byte[] getBytes(int idx) {
        checkIndex(idx);
        return record.getBytes(idx);
    }

    @Override
    public byte[] getBytes(String columnName) {
        return getBytes(getColumnIndex(columnName));
    }

    @Override
    public void set(Object[] values) {
        if (values.length > columns.length) {
            throw new IllegalArgumentException("not supported");
        }
    }

    @Override
    public Object[] toArray() {
        Object[] objects = new Object[columns.length];
        Object[] allObjects = record.toArray();
        for (int i = 0; i < columns.length; ++i) {
            objects[i] = allObjects[i];
        }
        return objects;
    }

    @Override
    public Record clone() {
        return new UpsertRecord(record.getColumns(), record.toArray());
    }

    private void checkIndex(int idx) {
        if (idx > columns.length) {
            throw new IllegalArgumentException("index out of range");
        }
    }

    private int getColumnIndex(String name) {
        Integer idx = nameMap.get(name);
        if (idx == null) {
            throw new IllegalArgumentException("No such column:" + name);
        }
        return idx;
    }
}
