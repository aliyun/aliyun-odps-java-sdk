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

package com.aliyun.odps.mapred.bridge;

public enum ErrorCode {
  CONNECTION_ERROR("ODPS-0700001", "Error connecting server"),

  INVALID_TABLE_NAME("ODPS-0720001", "Invalid table name"),

  TABLE_NOT_FOUND("ODPS-0720003", "Table not found"),

  VOLUME_NOT_FOUND("ODPS-0720004", "Volume not found"),

  MALFORMED_VOLUME_SPEC("ODPS-0720005", "Malformed volume spec"),

  DUPLICATED_VOLUME_FOUND("ODPS-0720005", "Duplicated volume definition found"),

  COLUMN_NOT_FOUND("ODPS-0720021", "Column not found"),

  PARTITION_COLUMN_NOT_FOUND("ODPS-0720022", "Partition column not found"),

  RESOURCE_NOT_FOUND("ODPS-0720027", "Resource not found"),

  TOO_MANY_RESOURCE_ITEMS("ODPS-0720028", "Too many resource items, should be less than 256"),

  VIEW_TABLE("ODPS-0720111", "Read/Write to virtual view is not permitted"),

  OUTPUT_LABEL_NOT_UNIQUE("ODPS-0720112",
                          "Multiple output tables share the same label. Forgot to assign label in multiple output?"),

  VOLUME_LABEL_NOT_UNIQUE("ODPS-0720113", "Multiple volumes share the same label"),

  ILLEGAL_CONFIG("ODPS-0720141", "Illegal config"),

  NO_INPUT_TABLE("ODPS-0720231", "No input table found"),

  TOO_MANY_INPUT_TABLE("ODPS-0720301", "Too many input tables"),

  TOO_MANY_INPUT_VOLUME("ODPS-0720303", "Too many input volumes"),

  TOO_MANY_OUTPUT_VOLUME("ODPS-0720304", "Too many output volumes"),

  // Server side exceptions.

  INTERMEDIATE_OUTPUT_IN_REDUCER("ODPS-0720501",
                                 "Unexpected intermediate output in reducer, do you mean write to destination by write(Record)?"),

  UNEXPECTED_MAP_WRITE_OUTPUT("ODPS-0720502",
                              "Unexpected output in mapper, it is only allowed in map only (reduce num = 0) jobs"),

  UNEXPECTED_MAP_WRITE_INTER("ODPS-0720503",
                             "Unexpected intermediate output in mapper, it is only allowed in mapreduce (reduce num > 0) jobs"),

  NO_SUCH_LABEL("ODPS-0720504", "No such label"),

  MAPPER_CLASS_NOT_FOUND("ODPS-0720505", "Mapper class not found"),

  REDUCER_CLASS_NOT_FOUND("ODPS-0720506", "Reducer class not found"),

  COMBINER_CLASS_NOT_FOUND("ODPS-0720507", "Combiner class not found"),

  TOO_MANY_COUNTERS("ODPS-0720601", "Too many counters, expecting less than 64"),

  COUNTERS_NAME_TOO_LONG("ODPS-0720602", "Name of counter too long, expecting less than 100"),

  INTERNAL_ERROR("ODPS-0725000", "Internal error"),

  UNEXPECTED("ODPS-0729999", "An unexpected error encounterred"),;

  public String code;
  public String msg;

  ErrorCode(String code, String msg) {
    this.code = code;
    this.msg = msg;
  }

  @Override
  public String toString() {
    return String.format("%s:%s", code, msg);
  }

}
