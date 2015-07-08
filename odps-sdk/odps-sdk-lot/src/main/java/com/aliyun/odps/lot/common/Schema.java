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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.OdpsType;

import apsara.odps.TypesProtos;
import apsara.odps.lot.SchemaProtos;

public class Schema {

  public static class Column {

    private OdpsType type;
    private String name;

    public Column(OdpsType type, String name) {
      if (name == null) {
        throw new ArgumentNullException("name");
      }

      this.type = type;
      this.name = name;
    }

    public OdpsType getType() {
      return type;
    }

    public String getName() {
      return name;
    }

    public static TypesProtos.Type castType(OdpsType type) {
      switch (type) {
        case BIGINT:
          return TypesProtos.Type.Integer;
        case DOUBLE:
          return TypesProtos.Type.Double;
        case BOOLEAN:
          return TypesProtos.Type.Bool;
        case DATETIME:
          return TypesProtos.Type.Datetime;
        case STRING:
          return TypesProtos.Type.String;
        default:
          throw new RuntimeException("Invalid type of Schema.Column.");
      }
    }

    public SchemaProtos.Schema.Columns toProtoBuf() {
      SchemaProtos.Schema.Columns.Builder builder = SchemaProtos.Schema.Columns.newBuilder();
      builder.setName(name);
      builder.setType(castType(type));

      return builder.build();
    }
  }

  public Schema() {
  }

  private List<Column> columns = new ArrayList<Column>();
  private Set<String> names = new HashSet<String>();

  public List<Column> getColumns() {
    return columns;
  }

  public void addColumn(OdpsType type, String name) throws IllegalOperationException {
    if (names.contains(name)) {
      throw new IllegalOperationException(
          "You can't add columns with the same name '" + name + "'.");
    }

    columns.add(new Column(type, name));
  }

  public SchemaProtos.Schema toProtoBuf() {
    SchemaProtos.Schema.Builder builder = SchemaProtos.Schema.newBuilder();
    for (Column col : columns) {
      builder.addColumns(col.toProtoBuf());
    }

    return builder.build();
  }
}
