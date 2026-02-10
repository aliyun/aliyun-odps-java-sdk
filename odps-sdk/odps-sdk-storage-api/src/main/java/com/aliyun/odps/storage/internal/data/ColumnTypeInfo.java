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

package com.aliyun.odps.storage.internal.data;

import java.util.List;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ColumnTypeInfo implements TypeInfo {

  @SerializedName("Type")
  private OdpsType type;

  @SerializedName("Precision")
  private int precision;

  @SerializedName("Scale")
  private int scale;

  @SerializedName("SpecifiedLength")
  private int specifiedLength;

  @SerializedName("MemberName")
  private String memberName;

  @SerializedName("SubTypes")
  private List<ColumnTypeInfo> subTypes;

  @SerializedName("Nullable")
  private boolean nullable;

  @SerializedName("ColumnId")
  private long columnId;

  @Override
  public String getTypeName() {
    return type.name();
  }

  @Override
  public OdpsType getOdpsType() {
    return type;
  }

  public OdpsType getType() {
    return type;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  public int getSpecifiedLength() {
    return specifiedLength;
  }

  public String getMemberName() {
    return memberName;
  }

  public List<ColumnTypeInfo> getSubTypes() {
    return subTypes;
  }

  public boolean isNullable() {
    return nullable;
  }

  public long getColumnId() {
    return columnId;
  }
}
