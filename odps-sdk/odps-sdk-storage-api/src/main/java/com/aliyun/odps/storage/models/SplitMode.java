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

package com.aliyun.odps.storage.models;

import java.lang.reflect.Type;

import com.aliyun.odps.storage.ClientException;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Enumeration of split modes for table read operations.
 * <p>
 * This enum defines the different ways data can be split when reading from a table,
 * such as by size, parallelism, row offset, or bucket.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public enum SplitMode {
  SIZE,
  PARALLELISM,
  ROW_OFFSET,
  BUCKET;

  public static SplitMode fromString(String mode) {
    switch (mode) {
      case "Size":
        return SIZE;
      case "Parallelism":
        return PARALLELISM;
      case "RowOffset":
        return ROW_OFFSET;
      case "Bucket":
        return BUCKET;
      default:
        throw new ClientException("Unexpected split mode");
    }
  }

  @Override
  public String toString() {
    switch (this) {
      case SIZE:
        return "Size";
      case PARALLELISM:
        return "Parallelism";
      case ROW_OFFSET:
        return "RowOffset";
      case BUCKET:
        return "Bucket";
      default:
        throw new ClientException("Unexpected split mode");
    }
  }

  public static class SplitModeSerializer implements JsonSerializer<SplitMode> {

    @Override
    public JsonElement serialize(SplitMode src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      return context.serialize(src.toString());
    }
  }

  public static class SplitModeDeserializer implements JsonDeserializer<SplitMode> {

    @Override
    public SplitMode deserialize(JsonElement json, Type typeOfT,
                                 JsonDeserializationContext context) throws JsonParseException {
      String str = json.getAsString();
      return SplitMode.fromString(str);
    }
  }
}