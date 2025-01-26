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

package com.aliyun.odps;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * TableSchema表示ODPS中表的定义
 */
public class TableSchema implements Serializable {
  private static final long serialVersionUID = 1L;
  private ArrayList<Column> columns = new ArrayList<Column>();
  private ArrayList<Column> partitionColumns = new ArrayList<Column>();

  private HashMap<String, Integer> nameMap = new HashMap<String, Integer>();
  private HashMap<String, Integer> partitionNameMap = new HashMap<String, Integer>();

  /**
   * 创建TableSchema对象
   */
  public TableSchema() {

  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * 表增加一列
   * 本方法不用于直接修改 SQL 表结构，适用于执行 MR 作业定义输入列等场景。
   *
   * @param c
   *     待新增的{@link Column}对象
   *
   * @throws IllegalArgumentException
   *     c为空、列名已存在或不合法
   */
  public void addColumn(Column c) {

    if (c == null) {
      throw new IllegalArgumentException("Column is null.");
    }

    if (nameMap.containsKey(c.getName())
        || partitionNameMap.containsKey(c.getName())) {
      throw new IllegalArgumentException("Column " + c.getName()
                                         + " duplicated.");
    }

    nameMap.put(c.getName(), columns.size());

    columns.add(c);
  }

  /**
   * 表增加一个分区列
   *
   * @param c
   *     {@link Column}对象
   * @throws IllegalArgumentException
   *     c为空、列名已存在或不合法
   */
  public void addPartitionColumn(Column c) {
    if (c == null) {
      throw new IllegalArgumentException("Column is null.");
    }

    if (nameMap.containsKey(c.getName())
        || partitionNameMap.containsKey(c.getName())) {
      throw new IllegalArgumentException("Column " + c.getName()
                                         + " duplicated.");
    }

    partitionNameMap.put(c.getName(), partitionColumns.size());

    partitionColumns.add(c);
  }

  /**
   * 获得列信息
   *
   * @param idx
   *     列索引值
   * @return 列信息{@link Column}对象
   */
  public Column getColumn(int idx) {
    if (idx < 0 || idx >= columns.size()) {
      throw new IllegalArgumentException("idx out of range");
    }

    return columns.get(idx);
  }

  /**
   * 取得列索引
   *
   * @param name
   *     列名
   * @return 列索引值
   * @throws IllegalArgumentException
   *     列不存在
   */
  public int getColumnIndex(String name) {
    Integer idx = nameMap.get(name);

    if (idx == null) {
      throw new IllegalArgumentException("No such column:" + name);
    }

    return idx;
  }

  /**
   * 取得列对象
   *
   * @param name
   *     列名
   * @return {@link Column}对象
   * @throws IllegalArgumentException
   *     列不存在
   */
  public Column getColumn(String name) {
    return columns.get(getColumnIndex(name));
  }

  public void setColumns(List<Column> columns) {
    this.nameMap.clear();
    this.columns.clear();
    for (Column column : columns) {
      addColumn(column);
    }

  }

  /**
   * 获得列定义列表
   *
   * <p>
   * 在返回的List上增加、删除元素不会导致TableSchema增加或减少Column。返回的List不包含分区列。
   * </p>
   *
   * @return 被复制的{@link Column}列表
   */
  @SuppressWarnings("unchecked")
  public List<Column> getColumns() {
    return (List<Column>) columns.clone();
  }

  @Deprecated
  public void setPartitionColumns(ArrayList<Column> partitionColumns) {
    setPartitionColumns((List<Column>)partitionColumns);
  }

  public void setPartitionColumns(List<Column> partitionColumns) {
    this.partitionNameMap.clear();
    this.partitionColumns.clear();
    for (Column column : partitionColumns) {
      addPartitionColumn(column);
    }
  }

  /**
   * 获得分区列定义列表
   *
   * <p>
   * 在返回的List上增加、删除元素不会导致TableSchema增加或减少Column
   * </p>
   *
   * @return 被复制的{@link Column}列表
   */
  @SuppressWarnings("unchecked")
  public List<Column> getPartitionColumns() {
    return (List<Column>) partitionColumns.clone();
  }


  public List<Column> getAllColumns() {
    List<Column> allColumns = getColumns();
    allColumns.addAll(partitionColumns);
    return allColumns;
  }
  /**
   * 获得分区列定义
   *
   * @param name
   *     列名
   * @return 指定的分区列 {@link Column}
   */
  public Column getPartitionColumn(String name) {
    return partitionColumns.get(getPartitionColumnIndex(name));
  }

  /**
   * 获得分区列id
   *
   * @param name
   *     列名
   * @return 指定的分区列索引值
   */
  public int getPartitionColumnIndex(String name) {
    return partitionNameMap.get(name);
  }

  /**
   * 判断是否包含对应列
   *
   * @param name
   *     列名
   * @return 如果包含指定列，则返回true，否则返回false
   */
  public boolean containsColumn(String name) {
    return nameMap.containsKey(name);
  }

  /**
   * 判断是否包含对应分区列
   *
   * @param name
   *     列名
   * @return 如果包含指定分区列，则返回true，否则返回false
   */
  public boolean containsPartitionColumn(String name) {
    return partitionNameMap.containsKey(name);
  }

  public Column getPartitionColumn(int idx) {
    if (idx < 0 || idx >= partitionColumns.size()) {
      throw new IllegalArgumentException("idx out of range");
    }
    return partitionColumns.get(idx);
  }

  public PartitionSpec generatePartitionSpec(Record record) {
    PartitionSpec spec = new PartitionSpec();
    for (Column column : partitionColumns) {
      if (column.getGenerateExpression() == null) {
        throw new IllegalArgumentException(
            "column " + column.getName() + " has no generate expression");
      }
      spec.set(column.getName(), column.getGenerateExpression().generate(record));
    }
    return spec;
  }

  public boolean basicallyEquals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableSchema)) {
      return false;
    }
    TableSchema that = (TableSchema) o;
    List<Column> columnsA = getAllColumns();
    List<Column> columnsB = that.getAllColumns();
    if (columnsA.size() != columnsB.size()) {
      return false;
    }
    for (int i = 0; i < columnsA.size(); i++) {
      Column columnA = columnsA.get(i);
      Column columnB = columnsB.get(i);
      if (!columnA.getName().equals(columnB.getName()) || !columnA.getTypeInfo().getTypeName()
          .equals(columnB.getTypeInfo().getTypeName())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableSchema that = (TableSchema) o;
    return Objects.equals(columns, that.columns) && Objects.equals(
        partitionColumns, that.partitionColumns) && Objects.equals(nameMap, that.nameMap)
           && Objects.equals(partitionNameMap, that.partitionNameMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns, partitionColumns, nameMap, partitionNameMap);
  }

  public static class Builder {

    private final TableSchema schema;

    public Builder() {
      schema = new TableSchema();
    }

    public Builder withColumns(List<Column> columns) {
      for (Column column : columns) {
        withColumn(column);
      }
      return this;
    }

    public Builder withColumn(Column column) {
      schema.addColumn(column);
      return this;
    }

    public Builder withPartitionColumn(Column column) {
      schema.addPartitionColumn(column);
      return this;
    }

    /**
     * easy method to add specific type column.
     * for more column type, please use withColumn
     * @see Column
     * @see com.aliyun.odps.Column.ColumnBuilder
     */

    public Builder withBigintColumn(String columnName) {
      schema.addColumn(new Column(columnName, TypeInfoFactory.BIGINT));
      return this;
    }

    public Builder withStringColumn(String columnName) {
      schema.addColumn(new Column(columnName, TypeInfoFactory.STRING));
      return this;
    }

    public Builder withDoubleColumn(String columnName) {
      schema.addColumn(new Column(columnName, TypeInfoFactory.DOUBLE));
      return this;
    }

    public Builder withDecimalColumn(String columnName) {
      schema.addColumn(new Column(columnName, TypeInfoFactory.DECIMAL));
      return this;
    }

    public Builder withDatetimeColumn(String columnName) {
      schema.addColumn(new Column(columnName, TypeInfoFactory.DATETIME));
      return this;
    }

    public Builder withBooleanColumn(String columnName) {
      schema.addColumn(new Column(columnName, TypeInfoFactory.BOOLEAN));
      return this;
    }

    public TableSchema build() {
      return schema;
    }
  }
}
