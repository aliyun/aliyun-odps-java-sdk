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

package com.aliyun.odps.sqa;

import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;

/**
 * A hacker class help SQLExecutor can select blob data by instance tunnel
 * Current logic is when result column contains BLOB, use storage api to download results.
 * In the future, all result will be downloaded by storage api and this class will delete.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class InternalBlobHelper {

  public static boolean containBlob(TableSchema schema) {
    for (Column col : schema.getColumns()) {
      if (containBlob(col.getTypeInfo())) {
        return true;
      }
    }
    return false;
  }

  private static boolean containBlob(TypeInfo typeInfo) {
    if (typeInfo == null) {
      return false;
    }
    if (typeInfo.getOdpsType() == OdpsType.BLOB) {
      return true;
    }
    if (typeInfo instanceof ArrayTypeInfo) {
      return containBlob(((ArrayTypeInfo) typeInfo).getElementTypeInfo());
    } else if (typeInfo instanceof MapTypeInfo) {
      MapTypeInfo mapType = (MapTypeInfo) typeInfo;
      return containBlob(mapType.getKeyTypeInfo()) || containBlob(mapType.getValueTypeInfo());
    } else if (typeInfo instanceof StructTypeInfo) {
      StructTypeInfo structType = (StructTypeInfo) typeInfo;
      List<TypeInfo> fieldTypes = structType.getFieldTypeInfos();
      for (TypeInfo fieldType : fieldTypes) {
        if (containBlob(fieldType)) {
          return true;
        }
      }
    }
    return false;
  }

}
