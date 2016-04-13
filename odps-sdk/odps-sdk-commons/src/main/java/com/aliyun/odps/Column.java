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

import java.util.List;

/**
 * Column表示ODPS中表的列定义
 */
public final class Column {

  private String name;
  private OdpsType type;
  private String comment;
  private String label;
  private List<OdpsType> genericOdpsTypeList;

  /**
   * 构造Column对象
   *
   * @param name
   *     列名
   * @param type
   *     列类型
   * @param comment
   *     列注释
   */
  public Column(String name, OdpsType type, String comment) {
    this(name, type, comment, (String)null, null);
  }

  Column(String name, OdpsType type, String comment, String label,
         List<OdpsType> genericOdpsTypeList) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.label = label;
    this.genericOdpsTypeList = genericOdpsTypeList;
  }
  
  /**
   * 构造Column对象
   *
   * @param name
   *     列名
   * @param type
   *     列类型
   */
  public Column(String name, OdpsType type) {
    this(name, type, null);
  }

  /**
   * 获得列名
   *
   * @return 列名
   */
  public String getName() {
    return name;
  }

  /**
   * 获得列类型
   *
   * @return 列类型{@link OdpsType}
   */
  public OdpsType getType() {
    return type;
  }

  /**
   * 获得列注释
   *
   * @return 列注释
   */
  public String getComment() {
    return comment;
  }

  /**
   * 获得列标签
   *
   * @return 列标签
   * @see #getCategoryLabel() 替代
   */
  @Deprecated
  public Long getLabel() {
    if (label == null) {
      return null;
    } else {
      try {
        return Long.parseLong(label);
      } catch (NumberFormatException e) {
        return 0L;
      }
    }
  }
  
  public String getCategoryLabel() {
    return label;
  }

  public List<OdpsType> getGenericTypeList() {
    return genericOdpsTypeList;
  }

  public void setGenericTypeList(List<OdpsType> genericOdpsTypeList) {
    this.genericOdpsTypeList = genericOdpsTypeList;
  }
}
