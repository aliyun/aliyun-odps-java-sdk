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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * TableSchema表示ODPS中表的定义
 */
public class TableSchema {

  private ArrayList<Column> columns = new ArrayList<Column>();
  private ArrayList<Column> partitionColumns = new ArrayList<Column>();

  private HashMap<String, Integer> nameMap = new HashMap<String, Integer>();
  private HashMap<String, Integer> partitionNameMap = new HashMap<String, Integer>();

  /**
   * 创建TableSchema对象
   */
  public TableSchema() {

  }

  /**
   * 表增加一列
   *
   * @param c
   *     待新增的{@link Column}对象
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

  public void setPartitionColumns(ArrayList<Column> partitionColumns) {
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
}
