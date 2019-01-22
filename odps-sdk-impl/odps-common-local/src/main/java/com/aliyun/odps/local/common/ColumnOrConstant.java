package com.aliyun.odps.local.common;

import com.aliyun.odps.type.TypeInfo;

public class ColumnOrConstant {

  private String colName;
  private Integer colIndex; // column index in table schema

  private Object constantValue;
  private TypeInfo constantTypeInfo; // constant type info

  public ColumnOrConstant(Object constantValue, TypeInfo constantTypeInfo) {
    this.constantValue = constantValue;
    this.constantTypeInfo = constantTypeInfo;
  }

  public ColumnOrConstant(String colName, Integer colIndex) {
    this.colName = colName;
    this.colIndex = colIndex;
  }

  public boolean isConstant() {
    return constantValue != null;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public Integer getColIndex() {
    return colIndex;
  }

  public void setColIndex(Integer colIndex) {
    this.colIndex = colIndex;
  }

  public Object getConstantValue() {
    return constantValue;
  }

  public void setConstantValue(Object constantValue) {
    this.constantValue = constantValue;
  }

  public TypeInfo getConstantTypeInfo() {
    return constantTypeInfo;
  }

  public void setConstantTypeInfo(TypeInfo constantTypeInfo) {
    this.constantTypeInfo = constantTypeInfo;
  }

}