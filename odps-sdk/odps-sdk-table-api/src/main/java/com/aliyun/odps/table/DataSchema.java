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

package com.aliyun.odps.table;

import com.aliyun.odps.Column;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Serialized table schema.
 */
public class DataSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Attribute> attributes;

    private final List<String> partitionKeys;

    private transient List<Column> columns;

    public DataSchema(List<Column> columns) {
        this(columns, new ArrayList<>());
    }

    public DataSchema(List<Column> columns, List<String> partitionKeys) {
        Preconditions.checkNotNull(columns, "Columns must not be null.");
        Preconditions.checkNotNull(partitionKeys, "PartitionKeys must not be null.");
        this.attributes = columns.stream()
                .map(DataSchema::columnToAttribute)
                .collect(Collectors.toList());
        this.columns = columns;
        this.partitionKeys = Collections.unmodifiableList(partitionKeys);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Columns\n");
        for (Attribute column : attributes) {
            sb.append(" |-- ");
            sb.append(column.toString());
            sb.append('\n');
        }

        if (!partitionKeys.isEmpty()) {
            sb.append("PartitionKeys\n");
            for (String partitionKey : partitionKeys) {
                sb.append(" |-- ");
                sb.append(partitionKey);
                sb.append('\n');
            }
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataSchema that = (DataSchema) o;
        return Objects.equals(attributes, that.attributes)
                && Objects.equals(partitionKeys, that.partitionKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes, partitionKeys);
    }

    private void readObject(ObjectInputStream inputStream)
            throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        this.columns = attributes.stream()
                .map(DataSchema::attributeToColumn)
                .collect(Collectors.toList());
    }

    public int getColumnCount() {
        return columns.size();
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public List<String> getColumnNames() {
        return columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    public List<TypeInfo> getColumnDataTypes() {
        return columns.stream()
                .map(Column::getTypeInfo)
                .collect(Collectors.toList());
    }

    public Optional<Column> getColumn(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columns.size()) {
            return Optional.empty();
        }
        return Optional.of(this.columns.get(columnIndex));
    }

    public Optional<Column> getColumn(String columnName) {
        return this.columns.stream()
                .filter(column -> column.getName().equals(columnName))
                .findFirst();
    }

    private static Attribute columnToAttribute(Column column) {
        return new Attribute(
                column.getName(),
                column.getTypeInfo().getTypeName(),
                column.isNullable());
    }

    private static Column attributeToColumn(Attribute attribute) {
        Column column = new Column(attribute.getName(),
                getTypeInfoFromString(attribute.getType()));
        column.setNullable(attribute.isNullable());
        return column;
    }

    private static TypeInfo getTypeInfoFromString(String typeName) {
        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeName);
        if (typeInfo == null) {
            throw new IllegalArgumentException("Parse odps type info failed: " + typeName);
        }
        return typeInfo;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private List<Column> columns;
        private List<String> partitionKeys;

        private Builder() {
            this.columns = new ArrayList<>();
            this.partitionKeys = new ArrayList<>();
        }

        public Builder columns(List<Column> columns) {
            this.columns = columns;
            return this;
        }

        public Builder partitionBy(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public DataSchema build() {
            return new DataSchema(columns, partitionKeys);
        }
    }

    public static class Attribute implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String name;
        private final String type;
        private final boolean nullable;

        public Attribute(String name,
                         String type,
                         boolean nullable) {
            this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
            this.type = Preconditions.checkNotNull(type, "Field data type must not be null.");
            this.nullable = nullable;
        }

        public final String getName() {
            return name;
        }

        public final String getType() {
            return type;
        }

        public boolean isNullable() {
            return nullable;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(name);
            sb.append(": ");
            sb.append(type);
            sb.append(": ");
            sb.append(nullable);
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Attribute that = (Attribute) o;
            // TODO: nullable
            return Objects.equals(name, that.name)
                    && Objects.equals(type, that.type);
        }

        @Override
        public int hashCode() {
            // TODO: nullable
            return Objects.hash(name, type);
        }
    }
}
