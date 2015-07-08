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

package com.aliyun.odps.lot.common;

import com.aliyun.odps.OdpsType;

import apsara.odps.lot.ExpressionProtos;

public class Constant extends ScalarExpression {

  public long getIntValue() {
    assert (getType() == OdpsType.BIGINT);
    return intValue;
  }

  public void setIntValue(long intValue) {
    this.intValue = intValue;
    odpsType = OdpsType.BIGINT;
  }

  public double getDoubleValue() {
    assert (getType() == OdpsType.DOUBLE);
    return doubleValue;
  }

  public void setDoubleValue(double doubleValue) {
    this.doubleValue = doubleValue;
    odpsType = OdpsType.DOUBLE;
  }

  public String getStringValue() {
    assert (getType() == OdpsType.STRING);
    return stringValue;
  }

  public void setStringValue(String stringValue) {
    this.stringValue = stringValue;
    odpsType = OdpsType.STRING;
  }

  public long getDatetimeValue() {
    assert (getType() == OdpsType.DATETIME);
    return datetimeValue;
  }

  public void setDatetimeValue(long datetimeValue) {
    this.datetimeValue = datetimeValue;
    odpsType = OdpsType.DATETIME;
  }

  public boolean getBoolValue() {
    assert (getType() == OdpsType.BOOLEAN);
    return boolValue;
  }

  public void setBoolValue(boolean boolValue) {
    this.boolValue = boolValue;
    odpsType = OdpsType.BOOLEAN;
  }

  private long intValue;
  private double doubleValue;
  private String stringValue;
  private long datetimeValue;
  private boolean boolValue;

  private OdpsType odpsType;

  public OdpsType getType() {
    return odpsType;
  }

  public Constant() {
    setIntValue(0);
  }

  @Override
  public ExpressionProtos.ScalarExpression toProtoBuf() {
    ExpressionProtos.ScalarExpression.Builder
        builder =
        ExpressionProtos.ScalarExpression.newBuilder();
    apsara.odps.ExpressionProtos.Constant.Builder
        c =
        apsara.odps.ExpressionProtos.Constant.newBuilder();
    switch (odpsType) {
      case BIGINT:
        c.setInteger(intValue);
        break;
      case DOUBLE:
        c.setDouble(doubleValue);
        break;
      case BOOLEAN:
        c.setBool(boolValue);
        break;
      case DATETIME:
        c.setDatetime(datetimeValue);
        break;
      case STRING:
        c.setString(stringValue);
        break;
      default:
        assert (false);
    }

    builder.setConstant(c.build());
    return builder.build();
  }
}
