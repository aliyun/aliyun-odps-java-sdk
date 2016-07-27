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

package com.aliyun.odps.data;

import java.math.BigDecimal;
import java.util.Date;

import com.aliyun.odps.Column;

/**
 * Record类的对象表示ODPS表中一条记录
 *
 * @see RecordReader#read
 * @see RecordWriter#write
 */
public interface Record {

  /**
   * 获得记录中包含的字段数量
   *
   * @return 字段{@link Column}数量
   */
  public int getColumnCount();

  /**
   * 获得记录中包含的所有字段
   *
   * @return {@link Column}[]
   */
  public Column[] getColumns();

  /**
   * 判断某列是否为NULL值，如果该列为null，则返回true
   * 
   * @param idx
   *          列序号，0起始
   * @return 如果该列为null，则返回true，否则false
   */
  public boolean isNull(int idx);

  /**
   * 判断某列是否为NULL值，如果该列为null，则返回true
   * 
   * @param columnName
   *          列名
   * @return 如果该列为null，则返回true，否则false
   */
  public boolean isNull(String columnName);

  /**
   * 设置列的值
   *
   * @param idx
   *     列的索引
   * @param value
   *     列的值 Object
   */
  public void set(int idx, Object value);

  /**
   * 获取当前列的值
   *
   * @param idx
   *     列的索引
   * @return 对应索引列值的Object对象
   */
  public Object get(int idx);

  /**
   * 设置列的值
   *
   * @param columnName
   *     列名
   * @param value
   *     列的值
   */
  public void set(String columnName, Object value);

  /**
   * 获取列值
   *
   * @param columnName
   *     列名
   * @return 对应列名的Object对象
   */
  public Object get(String columnName);


  /**
   * 设置对应索引列的值，该列必须为Bigint类型
   *
   * @param idx
   *     列索引
   * @param Bigint值
   * @see #set(int, Object)
   */
  public void setBigint(int idx, Long value);

  /**
   * 获取对应索引列的值，该列必须为Bigint类型
   *
   * @param idx
   *     列索引
   * @return 对应索引列的值
   * @see #get(int)
   */
  public Long getBigint(int idx);

  /**
   * 设置对应列名的值，该列必须为Bigint类型
   *
   * @param columnName
   *     列名
   * @param value
   *     列值
   * @see #set(String, Object)
   */
  public void setBigint(String columnName, Long value);

  /**
   * 获取对应列名的值，该列必须为Bigint类型
   *
   * @param columnName
   *     列名
   * @return 列值
   * @see #get(String)
   */
  public Long getBigint(String columnName);

  /**
   * 设置对应索引列的值，该列必须为Double类型
   *
   * @param idx
   *     列索引
   * @param value
   *     列值
   * @see #set(int, Object)
   */
  public void setDouble(int idx, Double value);

  /**
   * 获取对应索引列的值，该列必须为Double类型
   *
   * @param idx
   *     列索引
   * @return 列值
   * @see #get(int)
   */
  public Double getDouble(int idx);

  /**
   * 设置对应列名的值，该列必须为Double类型
   *
   * @param columnName
   *     列名
   * @param value
   *     列值
   * @see #set(String, Object)
   */
  public void setDouble(String columnName, Double value);

  /**
   * 设置对应列的值，该列必须为Double类型
   *
   * @param columnName
   *     列名
   * @return 列值
   * @see #get(String)
   */
  public Double getDouble(String columnName);

  /**
   * 设置对应索引列的值，该列必须为Boolean类型
   *
   * @param idx
   *     列索引
   * @param value
   *     列值
   * @see #set(int, Object)
   */
  public void setBoolean(int idx, Boolean value);

  /**
   * 设置对应索引列的值，该列必须为Boolean类型
   *
   * @param idx
   *     列索引
   * @return 列值
   * @see #get(int)
   */
  public Boolean getBoolean(int idx);

  /**
   * 设置对应列名的值，该列必须为Boolean类型
   *
   * @param columnName
   *     列名
   * @param value
   *     列值
   * @see #set(String, Object)
   */
  public void setBoolean(String columnName, Boolean value);

  /**
   * 获取对应列名的值，该列必须为Boolean类型
   *
   * @param columnName
   *     列名
   * @return 列值
   * @see #get(String)
   */
  public Boolean getBoolean(String columnName);

  /**
   * 设置对应索引列的值，该列必须为Datetime类型
   *
   * @param idx
   *     列索引
   * @param value
   *     列值
   * @see #set(int, Object)
   */
  public void setDatetime(int idx, Date value);

  /**
   * 获取对应索引列的值，该列必须为Datetime类型
   *
   * @param idx
   *     列索引
   * @return 列值
   * @see #get(int)
   */
  public Date getDatetime(int idx);

  /**
   * 设置对应列名的值，该列必须为Datetime类型
   *
   * @param columnName
   *     列名
   * @param value
   *     列值
   * @see #set(String, Object)
   */
  public void setDatetime(String columnName, Date value);

  /**
   * 获取对应列名的值，该列必须为Datetime类型
   *
   * @param columnName
   *     列名
   * @return 列值
   * @see #get(String)
   */
  public Date getDatetime(String columnName);

  /**
   * 设置对应索引列的值，该列必须为Decimal类型
   * Decimal 类型整数部分36位有效数字 小数部分保留18位有效数字
   * 与 ODPS 交互时 整数部分超过36位会发生异常， 小数部分超过18位会被截断
   * 使用 BigDecimal 需要注意，BigDecimal 精度与 ODPS Decimal 精度有差异
   * BigDecimal 精度为变长， ODPS Decimal 精度为定长18位
   * equals 判断时，不同精度的BigDecimal 会不等，建议使用 compareTo
   *
   * @param idx
   *     列索引
   * @param value
   *     列值
   * @see #set(int, Object)
   */
  public void setDecimal(int idx, BigDecimal value);

  /**
   * 获取对应索引列的值，该列必须为Decimal类型
   *
   * @param idx
   *     列索引
   * @return 列值
   * @see #get(int)
   */
  public BigDecimal getDecimal(int idx);

  /**
   * 设置对应列名的值，该列必须为Decimal类型
   * Decimal 类型整数部分36位有效数字 小数部分保留18位有效数字
   * 与 ODPS 交互时 整数部分超过36位会发生异常， 小数部分超过18位会被截断
   *
   * @param columnName
   *     列名
   * @param value
   *     列值
   * @see #set(String, Object)
   */
  public void setDecimal(String columnName, BigDecimal value);

  /**
   * 获取对应列名的值，该列必须为Decimal类型
   *
   * @param columnName
   *     列名
   * @return 列值
   * @see #get(String)
   */
  public BigDecimal getDecimal(String columnName);

  /**
   * 设置对应索引列的值，该列必须为String类型
   *
   * @param idx
   *     列索引
   * @param value
   *     列值
   * @see #set(int, Object)
   */
  public void setString(int idx, String value);

  /**
   * 获取对应索引列的值，该列必须为String类型
   *
   * @param idx
   *     列索引
   * @return 列值
   * @see #get(int)
   */
  public String getString(int idx);

  /**
   * 设置对应列名的值，该列必须为String类型
   *
   * @param columnName
   *     列名
   * @param value
   *     列值
   * @see #set(String, Object)
   */
  public void setString(String columnName, String value);

  /**
   * 获取对应列名的值，该列必须为String类型
   *
   * @param columnName
   *     列名
   * @return 列值
   * @see #get(String)
   */
  public String getString(String columnName);

  /**
   * 设置对应列索引的值，该列必须为String类型
   *
   * @param idx
   *     列索引
   * @param value
   *     列值
   * @see #set(int, Object)
   * 需要保证value.length就是需要传入的长度
   * 并且byte的值不会被复用
   */
  public void setString(int idx, byte[] value);

  /**
   * 设置对应列名的值，该列必须为String类型
   *
   * @see #set(String, Object)
   * 需要保证value.length就是需要传入的长度
   * 并且byte的值不会被复用
   */
  public void setString(String columnName, byte[] value);

  /**
   * 获取指定列索引的值
   *
   * @param idx
   *     列索引
   * @return 列值
   * @see #get(int)
   */
  public byte[] getBytes(int idx);

  /**
   * 获取指定列名的值
   *
   * @param columnName
   *     列名
   * @return 列值
   * @see #get(String)
   */
  public byte[] getBytes(String columnName);

  /**
   * 设置当前 {@link Record} 的所有 {@link Column} 的值， 数组大小请和Column大小保持一致
   *
   * @param values
   *     所有列的值
   */
  public void set(Object[] values);

  /**
   * 返回当前所有列值的数组
   */
  public Object[] toArray();

  /**
   * 生成当前Record的拷贝
   * 
   * @return 当前Record的拷贝
   */
  public Record clone();
}
