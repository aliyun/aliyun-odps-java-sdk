/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.table.record;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.AbstractChar;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.table.record.accessor.ArrowToRecordConverter;

public class ColumnarBatchRecord extends ArrayRecord {

    private final VectorSchemaRoot root;
    private int rowId;
    private ArrowVectorAccessor[] columnAccessors;
    private Map<String, Integer> nameMap = new HashMap<>();

    private void checkSchema(Schema schema, Column[] columns)  {
        if (columns.length != schema.getFields().size()) {
            throw new RuntimeException(
                    "The quality of field type is incompatible with the request schema!");
        }
        for (int i = 0; i < columns.length; ++i) {
            Field field = schema.getFields().get(i);
            if (!field.getName().equalsIgnoreCase(columns[i].getName())) {
                throw new RuntimeException(
                            "Required column is incompatible in arrow batch. Col: " + field.getName());
            }
        }
    }

    public ColumnarBatchRecord(VectorSchemaRoot root,
                               Column[] columns) {
        this(root, columns, 0);
        checkSchema(root.getSchema(), columns);

        columnAccessors = new ArrowVectorAccessor[columns.length];
        nameMap = new HashMap<>();

        List<FieldVector> fieldVectors = root.getFieldVectors();
        for (int i = 0; i < fieldVectors.size(); i++) {
            columnAccessors[i] = ArrowToRecordConverter.
                    createColumnVectorAccessor(fieldVectors.get(i), columns[i].getTypeInfo());
            nameMap.put(columns[i].getName().toLowerCase(), i);
        }
    }

    private ColumnarBatchRecord(VectorSchemaRoot root,
                                Column[] columns,
                                int rowId) {
        super(columns);
        this.root = root;
        this.rowId = rowId;
    }

    public void close() {
        if (root != null) {
            root.close();
        }
        this.rowId = 0;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    @Override
    public int getColumnCount() {
        return columnAccessors.length;
    }

    @Override
    public void set(int idx, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int idx) {
        try {
            return ArrowToRecordConverter.getData(columnAccessors[idx],
                    this.getColumns()[idx].getTypeInfo(),
                    rowId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(String columnName) {
        return get(getColumnIndex(columnName));
    }

    @Override
    public boolean isNull(int idx) {
        return columnAccessors[idx].isNullAt(rowId);
    }

    @Override
    protected <T> T getInternal(int idx) {
        try {
            return (T) ArrowToRecordConverter.getData(columnAccessors[idx],
                    this.getColumns()[idx].getTypeInfo(),
                    rowId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getString(int idx) {
        Object obj = getInternal(idx);

        if (obj == null) {
            return null;
        }

        if (obj instanceof byte []) {
            return bytesToString((byte []) obj);
        }

        return (String)obj;
    }

    @Override
    public byte[] getBytes(int idx) {
        Object obj = getInternal(idx);

        if (obj == null) {
            return null;
        }
        if (obj instanceof  byte[]) {
            return (byte[]) obj;
        } else if (obj instanceof String) {
            return stringToBytes((String) obj);
        } else if (obj instanceof Binary) {
            return ((Binary) obj).data();
        } else if (obj instanceof AbstractChar) {
            return stringToBytes(((AbstractChar)obj).getValue());
        }
        else {
            throw new RuntimeException("Does not support getBytes for type other than String/Binary/Char/VarChar, sees "
                    + obj.getClass());
        }
    }

    @Override
    public Record clone() {
        ArrayRecord record = new ArrayRecord(getColumns());
        for (int i = 0; i < getColumnCount(); i++) {
            if (isNull(i)) {
                record.set(i, null);
            } else {
                record.set(i, getInternal(i));
            }
        }
        return record;
    }

    @Override
    public void set(String columnName, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(Object[] values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    private int getColumnIndex(String name) {
        Integer idx = nameMap.get(name.toLowerCase());
        if (idx == null) {
            throw new IllegalArgumentException("No such column:" + name);
        }
        return idx;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index < getColumnCount(); index++) {
            Object o = get(index);
            if (o == null) {
                sb.append("null").append(",");
            } else if (o instanceof byte[]) {
                sb.append(bytesToString((byte[]) o)).append(",");
            } else {
                sb.append(o).append(",");
            }
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }
}
