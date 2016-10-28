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

package com.aliyun.odps.mapred.bridge.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.FieldSchema;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.bridge.WritableRecord;

/**
 * Adapt old Record definition to new API. Will remove this after jni
 * refactoring.
 */
public class VersionUtils {

  public static OdpsType getOdpsType(String str) {
    return OdpsType.valueOf(str.toUpperCase());
  }

  public static OdpsType[] getOdpsTypes(com.aliyun.odps.udf.OdpsType[] types) {
    OdpsType[] rt = new OdpsType[types.length];
    for (int i = 0; i < types.length; i++) {
      rt[i] = getOdpsType(types[i]);
    }
    return rt;
  }

  private static OdpsType getOdpsType(com.aliyun.odps.udf.OdpsType odpsType) {
    switch (odpsType) {
      case STRING:
        return OdpsType.STRING;
      case BIGINT:
        return OdpsType.BIGINT;
      case DOUBLE:
        return OdpsType.DOUBLE;
      case BOOLEAN:
        return OdpsType.BOOLEAN;
      default:
        throw new RuntimeException("Type " + odpsType + " not supported.");
    }
  }

  public static com.aliyun.odps.udf.OdpsType[] getOdpsTypes(OdpsType[] types) {
    com.aliyun.odps.udf.OdpsType[] rt = new com.aliyun.odps.udf.OdpsType[types.length];
    for (int i = 0; i < types.length; i++) {
      rt[i] = getOdpsType(types[i]);
    }
    return rt;
  }

  private static com.aliyun.odps.udf.OdpsType getOdpsType(OdpsType odpsType) {
    switch (odpsType) {
      case STRING:
        return com.aliyun.odps.udf.OdpsType.STRING;
      case BIGINT:
        return com.aliyun.odps.udf.OdpsType.BIGINT;
      case DOUBLE:
        return com.aliyun.odps.udf.OdpsType.DOUBLE;
      case BOOLEAN:
        return com.aliyun.odps.udf.OdpsType.BOOLEAN;
      default:
        throw new RuntimeException("Type " + odpsType + " not supported.");
    }
  }

  public static Record adaptRecord(com.aliyun.odps.Record r) {
    FieldSchema[] old = r.getFields();
    Column[] cols = new Column[old.length];
    for (int i = 0; i < old.length; i++) {
      cols[i] = adaptColumn(old[i]);
    }
    Record rt = new WritableRecord(cols);
    if (r.getAll() != null) {
      rt.set(r.getAll());
    }
    return rt;
  }

  private static Column adaptColumn(FieldSchema f) {
    return new Column(f.name, OdpsType.valueOf(f.type.toUpperCase()));
  }

}
