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

package com.aliyun.odps.io;

import java.io.IOException;

import com.aliyun.odps.Column;

/**
 * WritableRecord 表示 ODPS 表或分区的一条记录，一条记录由多列组成，列包括列名、类型和值.
 *
 * <p>
 * <ul>
 * <li>不包括分区列（Partition columns）
 * <li>{@link Writable} 用于表示列的取值
 * </ul>
 * Record支持通过列的序号（0起始）或列名操作记录的数据。
 * </p>
 *
 * <p>
 * {@link Writable} 子类和列类型的对应关系如下：
 * <ul>
 * <li>{@link LongWritable} - bigint
 * <li>{@link DoubleWritable} - double
 * <li>{@link Text} - string
 * <li>{@link BooleanWritable} - boolean
 * <li>{@link DatetimeWritable} - datetime
 * <li>{@link NullWritable} 或 null - NULL 值
 * </ul>
 * </p>
 */
public interface WritableRecord {

  /**
   * {@link #toDelimitedString()}和{@link #fromDelimitedString(String)}
   * 使用的默认列分隔符：CTRL + A
   */
  public final static char DELIM = '\u0001';

  /**
   * {@link #toDelimitedString()}和{@link #fromDelimitedString(String)}
   * 使用的默认NULL指示符: \N
   */
  public final static String NULLINDICATOR = "\\N";

  /**
   * 返回记录的列数，不包含分区列（Partition columns）
   *
   * @return 列数
   */
  public int size();

  /**
   * 判断某列是否为NULL值，如果该列为null或{@link NullWritable}，则返回true
   *
   * @param idx
   *     列序号，0起始
   * @return 如果该列为null或{@link NullWritable}，则返回true，否则false
   */
  public boolean isNull(int idx);

  /**
   * 判断某列是否为NULL值，如果该列为null或{@link NullWritable}，则返回true
   *
   * @param fieldName
   *     列名
   * @return 如果该列为null或{@link NullWritable}，则返回true，否则false
   */
  public boolean isNull(String fieldName) throws IOException;

  /**
   * 获取某列取值，如果index>=列数，抛{@link ArrayIndexOutOfBoundsException}异常
   *
   * @param index
   *     列序号，0起始
   * @return 列值
   */
  public Writable get(int index);

  /**
   * 获取某列取值，如果列名不存在，抛{@link IOException}异常
   *
   * @param fieldName
   *     列名
   * @return 列值
   * @throws IOException
   *     如果列名不存在，抛异常
   */
  public Writable get(String fieldName) throws IOException;

  /**
   * 设置列取值，如果index>=列数，抛{@link ArrayIndexOutOfBoundsException}异常
   *
   * @param index
   *     列序号，0起始
   * @param value
   *     列值
   */
  public void set(int index, Writable value);

  /**
   * 设置列取值
   *
   * @param fieldName
   *     列名
   * @param value
   *     列值
   * @throws IOException
   *     列名不存在，抛异常
   */
  public void set(String fieldName, Writable value) throws IOException;

  /**
   * 设置所有列的取值.
   *
   * <p>
   * 如果values数组大小和列数不一致，抛{@link IOException}异常.
   *
   * @param values
   *     所有列的取值
   * @throws IOException
   *     如果values数组大小和列数不一致，抛异常
   */
  public void set(Writable[] values) throws IOException;

  /**
   * 获取列属性.
   *
   * <p>
   * {@link Column}包含列名和类型，如果index>=列数，抛
   * {@link ArrayIndexOutOfBoundsException}异常
   *
   * @param index
   *     列序号
   * @return {@link Column}，包含列名和类型
   */
  public Column getField(int index);

  /**
   * 获取所有列属性，不包括分区列（Partition columns）
   *
   * @return {@link Column}数组
   */
  public Column[] getFields();

  /**
   * 获取所有列值，不包括分区列（Partition columns）
   *
   * @return {@link Writable}数组
   */
  public Writable[] getAll();


  /**
   * 生成当前WritableRecord的拷贝。
   *
   * @return 当前writableRecord的拷贝
   */
  public WritableRecord clone();
}
