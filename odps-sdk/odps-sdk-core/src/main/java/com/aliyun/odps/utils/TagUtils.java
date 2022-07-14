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

package com.aliyun.odps.utils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.ReloadException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tag;
import com.aliyun.odps.Tags.TagBuilder;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

/**
 * Tag utils
 */
public class TagUtils {

  public enum OPERATION_TYPE {
    // Set a tag/simple tag to an object
    SET,
    // Unset a tag/simple tag from an object
    UNSET
  }

  public enum OBJECT_TYPE {
    // Tables and partitions
    TABLE,
    // Functions
    FUNCTION,
    // Resources
    RESOURCE,
    // Volumes
    VOLUME
  }

  /**
   * This class represents a reference of an object ({@link Table}, {@link Resource}, etc.)
   */
  public static class ObjectRef {
    @Expose
    @SerializedName("ColumnNames")
    List<String> columns;

    @Expose
    @SerializedName("DatabaseName")
    String projectName;

    @Expose
    @SerializedName("ObjectName")
    String objectName;

    @Expose
    @SerializedName("ObjectType")
    OBJECT_TYPE objectType;

    public ObjectRef(
        OBJECT_TYPE type,
        String projectName,
        String objectName,
        List<String> columns) {
      this.objectType = Objects.requireNonNull(type);
      this.projectName = Objects.requireNonNull(projectName);
      this.objectName = Objects.requireNonNull(objectName);
      this.columns = columns;
    }
  }

  /**
   * This class represents a collection of simple tags
   */
  public static class SimpleTag {
    @Expose
    @SerializedName("Category")
    String category;

    @Expose
    @SerializedName("KeyValues")
    Map<String, String> kv;

    public SimpleTag(String category, Map<String, String> kv) {
      this.category = Objects.requireNonNull(category);
      this.kv = new HashMap(Objects.requireNonNull(kv));
    }
  }

  /**
   * This class represents a reference of a {@link Tag}
   */
  public static class TagRef {
    @Expose
    @SerializedName("Classification")
    String classificationName;

    @Expose
    @SerializedName("Name")
    String tagName;

    public TagRef(String classificationName, String tagName) {
      this.classificationName = Objects.requireNonNull(classificationName);
      this.tagName = Objects.requireNonNull(tagName);
    }
  }

  /**
   * This class represents the action of tagging an object.
   */
  public static class SetObjectTagInput {
    @Expose
    @SerializedName("OpType")
    OPERATION_TYPE op;

    @Expose
    @SerializedName("ObjectRef")
    ObjectRef objectRef;

    @Expose
    @SerializedName("TagRef")
    TagRef tagRef;

    @Expose
    @SerializedName("SimpleTag")
    SimpleTag simpleTag;

    public SetObjectTagInput (
        OPERATION_TYPE op,
        ObjectRef objectRef,
        TagRef tagRef,
        SimpleTag simpleTag) {
      this.op = Objects.requireNonNull(op);
      this.objectRef = Objects.requireNonNull(objectRef);

      // TagRef and SimpleTag should not be null at the same time
      if (tagRef == null && simpleTag == null) {
        throw new IllegalArgumentException(
            "TagRef and SimpleTag should not be null at the same time");
      }

      this.tagRef = tagRef;
      this.simpleTag = simpleTag;
    }
  }

  public static class SimpleTagInfoEntry {
    @Expose
    @SerializedName("TagCategory")
    String category;

    @Expose
    @SerializedName("TagName")
    String tagName;

    @Expose
    @SerializedName("TagValue")
    String tagValue;

    public String getCategory() {
      return category;
    }

    public String getTagName() {
      return tagName;
    }

    public String getTagValue() {
      return tagValue;
    }
  }

  public static class SimpleTagInfo {
    @Expose
    @SerializedName("Tags")
    Map<String, List<SimpleTagInfoEntry>> tags;

    @Expose
    @SerializedName("ColumnTags")
    Map<String, Map<String, List<SimpleTagInfoEntry>>> columnTags;

    @Expose
    @SerializedName("UpdateTime")
    Long updateTime;

    public Map<String, List<SimpleTagInfoEntry>> getTags() {
      return tags;
    }

    public Map<String, Map<String, List<SimpleTagInfoEntry>>> getColumnTags() {
      return columnTags;
    }

    public Long getUpdateTime() {
      return updateTime;
    }
  }

  public static class TagInfo {
    @Expose
    @SerializedName("Tags")
    Map<String, List<String>> tags;

    @Expose
    @SerializedName("ColumnTags")
    Map<String, Map<String, List<String>>> columnTags;

    @Expose
    @SerializedName("UpdateTime")
    Long updateTime;

    public Map<String, List<String>> getTags() {
      return tags;
    }

    public Map<String, Map<String, List<String>>> getColumnTags() {
      return columnTags;
    }

    public Long getUpdateTime() {
      return updateTime;
    }
  }

  /**
   * This class represents a collection of tags and simple tags on a certain object.
   */
  public static class ObjectTagInfo {
    @Expose
    @SerializedName("SimpleTag")
    SimpleTagInfo simpleTagInfo;

    @Expose
    @SerializedName("Tag")
    TagInfo tagInfo;

    public SimpleTagInfo getSimpleTagInfo() {
      return simpleTagInfo;
    }

    public TagInfo getTagInfo() {
      return tagInfo;
    }
  }

  public static ObjectTagInfo getObjectTagInfo(
      String resource,
      PartitionSpec partitionSpec,
      RestClient client) throws OdpsException {

    Objects.requireNonNull(resource);
    Objects.requireNonNull(client);

    Map<String, String> params = new HashMap<>();
    params.put("tagging", null);
    if (partitionSpec != null) {
      params.put("partition", partitionSpec.toString());
    }
    Response resp = client.request(resource, "GET", params, null, null);
    String body = new String(resp.getBody(), StandardCharsets.UTF_8);

    JsonParser jsonParser = new JsonParser();
    JsonElement jsonElement = jsonParser.parse(body);
    if (!jsonElement.isJsonObject()) {
      throw new ReloadException("Expect a JsonObject, but got: " + body);
    }

    JsonObject jsonObject = jsonElement.getAsJsonObject();

    if (jsonObject.has("ObjectTagInfo")) {
      return GsonObjectBuilder.get().fromJson(jsonObject.get("ObjectTagInfo"), ObjectTagInfo.class);
    } else {
      return null;
    }
  }

  public static List<Tag> getTags(ObjectTagInfo tagInfo, String columnName, Odps odps) {
    List<Tag> tags = new LinkedList<>();
    if (tagInfo.getTagInfo() != null
        && tagInfo.getTagInfo().getColumnTags() != null
        && tagInfo.getTagInfo().getColumnTags().get(columnName) != null) {
      for (Entry<String, List<String>> entry :
          tagInfo.getTagInfo().getColumnTags().get(columnName).entrySet()) {
        String classificationName = entry.getKey();
        for (String tagName : entry.getValue()) {
          tags.add(odps.classifications().get(classificationName).tags().get(tagName));
        }
      }
    }

    return tags;
  }

  public static List<Tag> getTags(ObjectTagInfo tagInfo, Odps odps) {
    List<Tag> tags = new LinkedList<>();
    if (tagInfo.getTagInfo() != null && tagInfo.getTagInfo().getTags() != null) {
      for (Entry<String, List<String>> entry : tagInfo.getTagInfo().getTags().entrySet()) {
        String classificationName = entry.getKey();
        for (String tagName : entry.getValue()) {
          tags.add(odps.classifications().get(classificationName).tags().get(tagName));
        }
      }
    }

    return tags;
  }

  public static Map<String, Map<String, String>> getSimpleTags(ObjectTagInfo tagInfo) {
    Map<String, Map<String, String>> simpleTags = new HashMap<>();
    if (tagInfo.getSimpleTagInfo() != null
        && tagInfo.getSimpleTagInfo().getTags() != null) {
      for (Entry<String, List<SimpleTagInfoEntry>> entry :
          tagInfo.getSimpleTagInfo().getTags().entrySet()) {
        String category = entry.getKey();
        for (SimpleTagInfoEntry simpleTagInfoEntry : entry.getValue()) {
          if (!simpleTags.containsKey(category)) {
            simpleTags.put(category, new HashMap<>());
          }
          simpleTags.get(category)
                    .put(simpleTagInfoEntry.getTagName(), simpleTagInfoEntry.getTagValue());
        }
      }
    }

    return simpleTags;
  }

  public static Map<String, Map<String, String>> getSimpleTags(
      ObjectTagInfo tagInfo,
      String columnName) {

    Map<String, Map<String, String>> simpleTags = new HashMap<>();
    if (tagInfo.getSimpleTagInfo() != null
        && tagInfo.getSimpleTagInfo().getColumnTags() != null
        && tagInfo.getSimpleTagInfo().getColumnTags().get(columnName) != null) {
      for (Entry<String, List<SimpleTagInfoEntry>> entry :
          tagInfo.getSimpleTagInfo().getColumnTags().get(columnName).entrySet()) {
        String category = entry.getKey();
        for (SimpleTagInfoEntry simpleTagInfoEntry : entry.getValue()) {
          if (!simpleTags.containsKey(category)) {
            simpleTags.put(category, new HashMap<>());
          }
          simpleTags.get(category)
                    .put(simpleTagInfoEntry.getTagName(), simpleTagInfoEntry.getTagValue());
        }
      }
    }

    return simpleTags;
  }

  public static void updateTagInternal(
      SetObjectTagInput setObjectTagInput,
      PartitionSpec partitionSpec,
      RestClient client) throws OdpsException {

    Objects.requireNonNull(setObjectTagInput);
    Objects.requireNonNull(client);

    OBJECT_TYPE type = setObjectTagInput.objectRef.objectType;
    String projectName = setObjectTagInput.objectRef.projectName;
    String objectName = setObjectTagInput.objectRef.objectName;

    String resource;
    switch (type) {
      // Table & partition
      case TABLE:
        resource = ResourceBuilder.buildTableResource(projectName, objectName);
        break;
      // Resource
      case RESOURCE:
        resource =
            ResourceBuilder.buildResourceResource(projectName, objectName);
        break;
      // Function
      case FUNCTION:
        resource =
            ResourceBuilder.buildFunctionResource(projectName, objectName);
        break;
      // Volume
      case VOLUME:
        resource = ResourceBuilder.buildVolumeResource(projectName, objectName);
        break;
      default:
        throw new IllegalArgumentException("Unsupported object type: " + type);
    }

    Map<String, String> params = new HashMap<>();
    params.put("tagging", null);
    if (partitionSpec != null) {
      params.put("partition", partitionSpec.toString());
    }
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/json");
    JsonObject jsonObject = new JsonObject();
    jsonObject.add(
        "SetObjectTagInput",
        GsonObjectBuilder.get().toJsonTree(setObjectTagInput));
    String body = GsonObjectBuilder.get().toJson(jsonObject);
    client.request(resource, "PUT", params, headers, body.getBytes());
  }

  public static void validateTaggingColumn(TableSchema schema, List<String> columnNames) {
    Objects.requireNonNull(schema);

    if (columnNames != null) {
      List<Column> columns = schema.getColumns();
      List<Column> partitionColumns = schema.getPartitionColumns();
      for (String columnName : columnNames) {
        if (partitionColumns.stream().anyMatch(c -> columnName.equalsIgnoreCase(c.getName()))) {
          throw new IllegalArgumentException("Partition column is unsupported yet.");
        }
        if (columns.stream().noneMatch(c -> columnName.equalsIgnoreCase(c.getName()))) {
          throw new IllegalArgumentException("Column does not exist: " + columnName + ".");
        }
      }
    }
  }

}
