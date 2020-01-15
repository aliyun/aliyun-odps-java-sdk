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
import java.util.List;

import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * Column表示ODPS中表的列定义
 */
public final class Column {

  private String name;
  private OdpsType type;
  private TypeInfo typeInfo;
  private String comment;
  private String label;

  private String defaultValue = null;
  private boolean isNullable = true;
  private boolean hasDefaultValue = false;

  private List<OdpsType> genericOdpsTypeList;
  private List<String> extendedlabels;

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
  @Deprecated
  public Column(String name, OdpsType type, String comment) {
    this(name, type, comment, (String) null, null);
  }

  public Column(String name, TypeInfo typeInfo, String comment) {
    this(name, typeInfo, comment, (String) null);
  }

  public Column(String name, TypeInfo typeInfo) {
    this(name, typeInfo, null);
  }

  public Column(String name, TypeInfo typeInfo, String comment, String label) {
    this(name, typeInfo, comment, label, null);
  }


  public Column(String name, TypeInfo typeInfo, String comment, String label, List<String> extendedlabels) {
    this.name = name;
    this.comment = comment;
    this.typeInfo = typeInfo;
    this.label = label;
    this.type = typeInfo.getOdpsType();
    this.extendedlabels = extendedlabels;

    // if it is array or map, should init genericOdpsTypeList.
    // otherwise, getGenericTypeList returns null when init column by this constructor
    initGenericOdpsTypeList();
  }

  private void initGenericOdpsTypeList() {
    switch (type) {
      case ARRAY: {
        genericOdpsTypeList = new ArrayList<OdpsType>();
        genericOdpsTypeList.add(((ArrayTypeInfo)typeInfo).getElementTypeInfo().getOdpsType());
        break;
      }
      case MAP: {
        genericOdpsTypeList = new ArrayList<OdpsType>();
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;

        genericOdpsTypeList.add(mapTypeInfo.getKeyTypeInfo().getOdpsType());
        genericOdpsTypeList.add(mapTypeInfo.getValueTypeInfo().getOdpsType());
        break;
      }
    }
  }

  public Column(String name, OdpsType type, String comment, String label,
         List<OdpsType> genericOdpsTypeList) {
    this.name = name;
    this.comment = comment;
    this.label = label;
    this.type = type;
    this.genericOdpsTypeList = genericOdpsTypeList;

    // if genericOdpsTypeList is null, array and map typeinfo object is null
    // it will create when setGenericOdpsTypeList
    initTypeInfo();
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

  private void initTypeInfo() {
    switch (type) {
      case ARRAY: {
        initArrayTypeInfo();
        break;
      }
      case MAP: {
        initMapTypeInfo();
        break;
      }
      case VARCHAR: {
        throw new IllegalArgumentException("The length of " + type
                                           + " must be specified, pls use TypeInfoFactory.getVarcharTypeInfo to new Column.");
      }
      case CHAR: {
        throw new IllegalArgumentException("The length of " + type
                                           + " must be specified, pls use TypeInfoFactory.getCharTypeInfo to new Column.");
      }
      default:
        if (typeInfo == null) {
          typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(type);
        }
        break;
    }
  }

  private void initMapTypeInfo() {
    if (genericOdpsTypeList == null) {
      return;
    }

    if (genericOdpsTypeList.size() < 2) {
      throw new IllegalArgumentException("Error genericOdpsTypeList for Map.");
    }
    TypeInfo keyType =
        TypeInfoFactory.getPrimitiveTypeInfo(genericOdpsTypeList.get(0));
    TypeInfo valueType =
        TypeInfoFactory.getPrimitiveTypeInfo(genericOdpsTypeList.get(1));

    typeInfo = TypeInfoFactory.getMapTypeInfo(keyType, valueType);
  }

  private void initArrayTypeInfo() {
    if (genericOdpsTypeList == null) {
      return;
    }

    if (genericOdpsTypeList.size() < 1) {
      throw new IllegalArgumentException("Error genericOdpsTypeList for Array.");
    }

    TypeInfo valueType =
        TypeInfoFactory.getPrimitiveTypeInfo(genericOdpsTypeList.get(0));

    typeInfo = TypeInfoFactory.getArrayTypeInfo(valueType);
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
  @Deprecated
  public OdpsType getType() {
    return type;
  }

  /**
   * 获得列类型
   *
   * @return 列类型{@link TypeInfo}
   */
  public TypeInfo getTypeInfo() {
    // if the GenericTypeList have not set before, the typeInfo is null for array and map type
    if (typeInfo == null) {
      throw new IllegalArgumentException(
          "Failed to get TypeInfo for " + type.toString() + ", please set generic type list first.");
    }

    return typeInfo;
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

  /**
   * 获取 Column 的扩展标签
   *
   * @return 列扩展标签
   */
  public List<String> getExtendedlabels() {
    return extendedlabels;
  }

  public String getCategoryLabel() {
    return label;
  }

  public List<OdpsType> getGenericTypeList() {
    return genericOdpsTypeList;
  }

  public void setGenericTypeList(List<OdpsType> genericOdpsTypeList) {
    // for array and map, compatible to OdpsType enum
    this.genericOdpsTypeList = genericOdpsTypeList;
    initTypeInfo();
  }

  /**
   * 获取 Column 的默认值， 若没有设置，则返回 NULL
   * 注意: 目前不论 column type 是什么，都返回的是字符串形式的默认值
   *
   * @return 列默认值
   */
  public String getDefaultValue()
  {
    return defaultValue;
  }

  /**
   * 设置 Column 的默认值
   * 注意: 目前不论 column type 是什么，都是字符串形式的默认值
   * @param defaultValue
   */
  public void setDefaultValue(String defaultValue)
  {
    this.defaultValue = defaultValue;

    if (defaultValue != null) {
      this.hasDefaultValue = true;
    } else {
      this.hasDefaultValue = false;
    }
  }

  /**
   * Column 是否可以为 NULL
   *
   * @return 是否可以是 null
   */
  public boolean isNullable()
  {
    return isNullable;
  }

  /**
   * 设置 Column 是否可以为 null
   * @param nullable
   */
  public void setNullable(boolean nullable)
  {
    isNullable = nullable;
  }

  /**
   * Column 是否有默认值
   *
   * @return 是否有默认值
   */
  public boolean hasDefaultValue()
  {
    return hasDefaultValue;
  }

}
