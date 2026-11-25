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

package com.aliyun.odps.storage.internal;

import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.Map;

/**
 * Explicit version-one session description. Never serializes a client or arbitrary response fields.
 */
public final class SessionDescriptorCodec {
  public static final String READ = "batch-read";
  public static final String INCREMENTAL = "incremental-read";
  public static final String WRITE = "batch-write";
  private final JsonObject value;

  // These names define the SDK descriptor schema, independently of response evolution.
  private static final String[] ROOT = {
    "SessionId",
    "SessionStatus",
    "DataSchema",
    "SupportedDataFormat",
    "SplitMode",
    "SplitsCount",
    "RecordCount",
    "SplitBucketId",
    "SessionStats",
    "ExtendedArrowIPCEnabled",
    "EnableLargeString",
    "LatestVersion",
    "LatestTxnId",
    "ExpirationTime",
    "IncrementalReadOptions",
    "MaxBlockNumber",
    "EnhanceWriteCheck",
    "RequiredOrdering",
    "RequiredDistribution",
    "CommitStarted"
  };
  private static final Map<String, String[]> CHILDREN = new HashMap<>();

  static {
    CHILDREN.put(
        "DataSchema",
        new String[] {"DataColumns", "PartitionColumns", "SystemColumns", "BlobColumns"});
    String[] column = {"Name", "Type", "Comment", "Nullable", "ColumnId"};
    CHILDREN.put("DataColumns", column);
    CHILDREN.put("PartitionColumns", column);
    CHILDREN.put("SystemColumns", column);
    CHILDREN.put("BlobColumns", column);
    CHILDREN.put("SupportedDataFormat", new String[] {"Type", "Version"});
    CHILDREN.put("SessionStats", new String[] {"EstimatedSize", "EstimatedRowCount"});
    CHILDREN.put(
        "IncrementalReadOptions",
        new String[] {
          "StartTimeStamp", "EndTimeStamp", "StartVersion", "EndVersion", "Mode", "StartTxnId"
        });
    CHILDREN.put("RequiredOrdering", new String[] {"Name", "SortDirection", "NullOrdering"});
    CHILDREN.put("RequiredDistribution", new String[] {"Type", "ClusterKeys", "BucketsNumber"});
  }

  private SessionDescriptorCodec(JsonObject value) {
    this.value = value;
  }

  public static SessionDescriptorCodec create(
      String kind,
      TableIdentifier table,
      JsonObject metadata,
      String routeToken,
      ArrowOptions arrow) {
    JsonObject root = new JsonObject();
    root.addProperty("version", 1);
    root.addProperty("provider", "v2");
    root.addProperty("kind", kind);
    JsonObject id = new JsonObject();
    id.addProperty("project", table.getProject());
    id.addProperty("schema", table.getSchema());
    id.addProperty("table", table.getTable());
    root.add("table", id);
    root.add("metadata", transform(metadata, ROOT, true));
    if (routeToken != null) root.addProperty("routeToken", routeToken);
    if (arrow != null) {
      JsonObject options = new JsonObject();
      options.addProperty("timestampUnit", arrow.getTimestampUnit().name());
      options.addProperty("datetimeUnit", arrow.getDateTimeUnit().name());
      root.add("arrow", options);
    }
    return parse(root.toString());
  }

  public static SessionDescriptorCodec parse(String json) {
    try {
      return parseDescription(json);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid or unsupported v2 session descriptor", e);
    }
  }

  private static SessionDescriptorCodec parseDescription(String json) {
    JsonObject root = new JsonParser().parse(json).getAsJsonObject();
    if (!root.has("version")
        || !root.get("version").isJsonPrimitive()
        || !root.get("version").getAsJsonPrimitive().isNumber()
        || !root.get("version").getAsString().equals("1"))
      throw new IllegalArgumentException("Unsupported session descriptor version");
    if (!"v2".equals(required(root, "provider")))
      throw new IllegalArgumentException("Expected v2 session descriptor");
    String kind = required(root, "kind");
    if (!READ.equals(kind) && !INCREMENTAL.equals(kind) && !WRITE.equals(kind))
      throw new IllegalArgumentException("Unsupported session descriptor kind");
    JsonObject id = root.getAsJsonObject("table");
    required(id, "project");
    required(id, "schema");
    required(id, "table");
    JsonObject metadata = root.getAsJsonObject("metadata");
    required(metadata, "sessionId");
    required(metadata, "sessionStatus");
    if (!metadata.has("dataSchema") || !metadata.get("dataSchema").isJsonObject())
      throw new IllegalArgumentException("Missing session descriptor dataSchema");
    SessionDescriptorCodec descriptor = new SessionDescriptorCodec(root);
    if (WRITE.equals(kind)) {
      descriptor.arrowOptions();
    } else if ("Bucket".equals(required(metadata, "splitMode"))) {
      int count = metadata.get("splitsCount").getAsInt();
      JsonArray buckets = metadata.getAsJsonArray("splitBucketId");
      if (count < 0 || buckets == null || count != buckets.size()) {
        throw new IllegalArgumentException("Bucket count must match splitsCount");
      }
      for (JsonElement bucket : buckets) {
        if (bucket.isJsonNull()
            || !bucket.isJsonPrimitive()
            || !bucket.getAsJsonPrimitive().isNumber()
            || !bucket.getAsString().matches("[0-9]+")) {
          throw new IllegalArgumentException("Invalid bucket id");
        }
        Integer.parseInt(bucket.getAsString());
      }
    }
    if (!WRITE.equals(kind)) {
      String mode = required(metadata, "splitMode");
      if ("Size".equals(mode) || "Bucket".equals(mode)) {
        if (nonNegativeInteger(metadata, "splitsCount") > Integer.MAX_VALUE) {
          throw new IllegalArgumentException("splitsCount exceeds supported range");
        }
      } else if ("RowOffset".equals(mode)) {
        nonNegativeInteger(metadata, "recordCount");
      } else {
        throw new IllegalArgumentException("Unsupported read split mode");
      }
    }
    descriptor.rowsPerSplit();
    return descriptor;
  }

  private static long nonNegativeInteger(JsonObject object, String key) {
    String text = required(object, key);
    if (!object.get(key).getAsJsonPrimitive().isNumber() || !text.matches("[0-9]+")) {
      throw new IllegalArgumentException("Invalid non-negative integer: " + key);
    }
    return Long.parseLong(text);
  }

  private static String required(JsonObject object, String key) {
    if (object == null
        || !object.has(key)
        || object.get(key).isJsonNull()
        || !object.get(key).isJsonPrimitive()
        || object.get(key).getAsString().isEmpty())
      throw new IllegalArgumentException("Missing session descriptor field: " + key);
    return object.get(key).getAsString();
  }

  // Nested schema fields are also allowlisted. Unknown server fields cannot leak into snapshots.
  private static JsonObject transform(JsonObject source, String[] names, boolean encode) {
    JsonObject result = new JsonObject();
    for (String wire : names) {
      String canonical = Character.toLowerCase(wire.charAt(0)) + wire.substring(1);
      String from = encode ? wire : canonical;
      if (!source.has(from) || source.get(from).isJsonNull()) continue;
      JsonElement element = source.get(from);
      if (CHILDREN.containsKey(wire)) {
        if (element.isJsonArray()) {
          JsonArray array = new JsonArray();
          for (JsonElement item : element.getAsJsonArray())
            array.add(transform(item.getAsJsonObject(), CHILDREN.get(wire), encode));
          element = array;
        } else element = transform(element.getAsJsonObject(), CHILDREN.get(wire), encode);
      }
      result.add(encode ? canonical : wire, element.deepCopy());
    }
    return result;
  }

  public String kind() {
    return value.get("kind").getAsString();
  }

  public TableIdentifier table() {
    JsonObject table = value.getAsJsonObject("table");
    return TableIdentifier.of(
        required(table, "project"), required(table, "schema"), required(table, "table"));
  }

  public JsonObject metadata() {
    return transform(value.getAsJsonObject("metadata"), ROOT, false);
  }

  public String routeToken() {
    return value.has("routeToken") ? value.get("routeToken").getAsString() : null;
  }

  public ArrowOptions arrowOptions() {
    JsonObject arrow = value.getAsJsonObject("arrow");
    return ArrowOptions.newBuilder()
        .withTimestampUnit(ArrowOptions.TimestampUnit.valueOf(required(arrow, "timestampUnit")))
        .withDatetimeUnit(ArrowOptions.TimestampUnit.valueOf(required(arrow, "datetimeUnit")))
        .build();
  }

  /** Internal native read planning option; not credentials or runtime resources. */
  public SessionDescriptorCodec withRowsPerSplit(long rows) {
    if (rows <= 0) throw new IllegalArgumentException("rowsPerSplit must be positive");
    JsonObject copy = value.deepCopy();
    JsonObject options = new JsonObject();
    options.addProperty("rowsPerSplit", rows);
    copy.add("readOptions", options);
    return parse(copy.toString());
  }

  public long rowsPerSplit() {
    if (!value.has("readOptions")) return 0;
    long rows = nonNegativeInteger(value.getAsJsonObject("readOptions"), "rowsPerSplit");
    if (rows <= 0) throw new IllegalArgumentException("rowsPerSplit must be positive");
    return rows;
  }

  public String toJson() {
    return value.toString();
  }
}
