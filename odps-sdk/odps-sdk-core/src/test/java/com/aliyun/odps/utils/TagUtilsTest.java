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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.utils.TagUtils.OBJECT_TYPE;
import com.aliyun.odps.utils.TagUtils.OPERATION_TYPE;
import com.aliyun.odps.utils.TagUtils.ObjectRef;
import com.aliyun.odps.utils.TagUtils.ObjectTagInfo;
import com.aliyun.odps.utils.TagUtils.SetObjectTagInput;
import com.aliyun.odps.utils.TagUtils.SimpleTag;
import com.aliyun.odps.utils.TagUtils.SimpleTagInfo;
import com.aliyun.odps.utils.TagUtils.SimpleTagInfoEntry;
import com.aliyun.odps.utils.TagUtils.TagInfo;
import com.aliyun.odps.utils.TagUtils.TagRef;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TagUtilsTest {
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  @Test
  public void testSetObjectTagInputSerialization() {
    ObjectRef objectRef = new ObjectRef(
        OBJECT_TYPE.TABLE,
        "test_project",
        "test_table",
        null);
    TagRef tagRef = new TagRef("test_classification", "test_tag");
    SimpleTag simpleTag = new SimpleTag(
        "test_category",
        Collections.singletonMap("key", "value"));
    SetObjectTagInput setObjectTagInput =
        new SetObjectTagInput(OPERATION_TYPE.SET, objectRef, tagRef, simpleTag);

    System.out.println(GSON.toJson(setObjectTagInput));
  }

  @Test
  public void testObjectTagInfoDeserialization() {
    String json =
        "{\n"
        + "    \"SimpleTag\": {\n"
        + "      \"Tags\": {\n"
        + "        \"PAI\": [\n"
        + "          {\n"
        + "            \"TagCategory\": \"PAI\",\n"
        + "            \"TagName\": \"pai_key-1\",\n"
        + "            \"TagValue\": \"pai_value-1\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"TagCategory\": \"PAI\",\n"
        + "            \"TagName\": \"pai_key-2\",\n"
        + "            \"TagValue\": \"pai_value-2\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"TagCategory\": \"PAI\",\n"
        + "            \"TagName\": \"pai_key-3\",\n"
        + "            \"TagValue\": \"pai_value-3\"\n"
        + "          },\n"
        + "          {\n"
        + "            \"TagCategory\": \"PAI\",\n"
        + "            \"TagName\": \"pai_key-4\",\n"
        + "            \"TagValue\": \"pai_value-4\"\n"
        + "          }\n"
        + "        ]\n"
        + "      },\n"
        + "      \"ColumnTags\": {\n"
        + "        \"a\": {\n"
        + "          \"PAI\": [\n"
        + "            {\n"
        + "              \"TagCategory\": \"PAI\",\n"
        + "              \"TagName\": \"pai_key-1\",\n"
        + "              \"TagValue\": \"pai_value-1\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"TagCategory\": \"PAI\",\n"
        + "              \"TagName\": \"pai_key-2\",\n"
        + "              \"TagValue\": \"pai_value-2\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"TagCategory\": \"PAI\",\n"
        + "              \"TagName\": \"pai_key-3\",\n"
        + "              \"TagValue\": \"pai_value-3\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"TagCategory\": \"PAI\",\n"
        + "              \"TagName\": \"pai_key-4\",\n"
        + "              \"TagValue\": \"pai_value-4\"\n"
        + "            }\n"
        + "          ]\n"
        + "        },\n"
        + "        \"b\": {\n"
        + "          \"PAI\": [\n"
        + "            {\n"
        + "              \"TagCategory\": \"PAI\",\n"
        + "              \"TagName\": \"pai_key-1\",\n"
        + "              \"TagValue\": \"pai_value-1\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"TagCategory\": \"PAI\",\n"
        + "              \"TagName\": \"pai_key-2\",\n"
        + "              \"TagValue\": \"pai_value-2\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"TagCategory\": \"PAI\",\n"
        + "              \"TagName\": \"pai_key-3\",\n"
        + "              \"TagValue\": \"pai_value-3\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"TagCategory\": \"PAI\",\n"
        + "              \"TagName\": \"pai_key-4\",\n"
        + "              \"TagValue\": \"pai_value-4\"\n"
        + "            }\n"
        + "          ]\n"
        + "        }\n"
        + "      },\n"
        + "      \"UpdateTime\": 1607483676\n"
        + "    },\n"
        + "    \"Tag\": {\n"
        + "      \"Tags\": {\n"
        + "        \"test_classification\": [\n"
        + "          \"test_tag_1\"\n"
        + "        ]\n"
        + "      },\n"
        + "      \"ColumnTags\": {\n"
        + "        \"a\": {\n"
        + "          \"test_classification\": [\n"
        + "            \"test_tag_1\"\n"
        + "          ]\n"
        + "        },\n"
        + "        \"b\": {\n"
        + "          \"test_classification\": [\n"
        + "            \"test_tag_1\"\n"
        + "          ]\n"
        + "        }\n"
        + "      },\n"
        + "      \"UpdateTime\": 1607483676\n"
        + "    }\n"
        + "  }";

    ObjectTagInfo objectTagInfo = GSON.fromJson(json, ObjectTagInfo.class);
    Assert.assertNotNull(objectTagInfo);

    // SimpleTag
    SimpleTagInfo simpleTagInfo = objectTagInfo.getSimpleTagInfo();
    Assert.assertNotNull(simpleTagInfo);
    // SimpleTag.Tags
    Map<String, List<SimpleTagInfoEntry>> categoryToSimpleTags = simpleTagInfo.getTags();
    Assert.assertNotNull(categoryToSimpleTags);
    Assert.assertEquals(1, categoryToSimpleTags.size());
    List<SimpleTagInfoEntry> simpleTagInfoEntries = categoryToSimpleTags.get("PAI");
    Assert.assertNotNull(simpleTagInfoEntries);
    Assert.assertEquals(4, simpleTagInfoEntries.size());
    for (int i = 1; i <= 4; i++) {
      SimpleTagInfoEntry entry = simpleTagInfoEntries.get(i - 1);
      Assert.assertEquals("PAI", entry.getCategory());
      Assert.assertEquals("pai_key-" + i, entry.getTagName());
      Assert.assertEquals("pai_value-" + i, entry.getTagValue());
    }
    // SimpleTag.ColumnTags
    Map<String, Map<String, List<SimpleTagInfoEntry>>> columnToCategoryToSimpleTags = simpleTagInfo.getColumnTags();
    Assert.assertNotNull(columnToCategoryToSimpleTags);
    Assert.assertEquals(2, columnToCategoryToSimpleTags.size());
    // SimpleTag.ColumnTags.a
    Assert.assertTrue(columnToCategoryToSimpleTags.containsKey("a"));
    Assert.assertEquals(1, columnToCategoryToSimpleTags.get("a").size());
    simpleTagInfoEntries = columnToCategoryToSimpleTags.get("a").get("PAI");
    Assert.assertNotNull(simpleTagInfoEntries);
    Assert.assertEquals(4, simpleTagInfoEntries.size());
    for (int i = 1; i <= 4; i++) {
      SimpleTagInfoEntry entry = simpleTagInfoEntries.get(i - 1);
      Assert.assertEquals("PAI", entry.getCategory());
      Assert.assertEquals("pai_key-" + i, entry.getTagName());
      Assert.assertEquals("pai_value-" + i, entry.getTagValue());
    }
    // SimpleTag.ColumnTags.b
    Assert.assertTrue(columnToCategoryToSimpleTags.containsKey("b"));
    Assert.assertEquals(1, columnToCategoryToSimpleTags.get("b").size());
    simpleTagInfoEntries = columnToCategoryToSimpleTags.get("b").get("PAI");
    Assert.assertNotNull(simpleTagInfoEntries);
    Assert.assertEquals(4, simpleTagInfoEntries.size());
    for (int i = 1; i <= 4; i++) {
      SimpleTagInfoEntry entry = simpleTagInfoEntries.get(i - 1);
      Assert.assertEquals("PAI", entry.getCategory());
      Assert.assertEquals("pai_key-" + i, entry.getTagName());
      Assert.assertEquals("pai_value-" + i, entry.getTagValue());
    }
    // SimpleTag.UpdateTime
    Assert.assertEquals(Long.valueOf(1607483676), simpleTagInfo.getUpdateTime());

    // Tag
    TagInfo tagInfo = objectTagInfo.getTagInfo();
    Assert.assertNotNull(tagInfo);
    Map<String, List<String>> classificationToTags = tagInfo.getTags();
    Assert.assertNotNull(classificationToTags);
    Assert.assertEquals(1, classificationToTags.size());
    Assert.assertNotNull(classificationToTags.get("test_classification"));
    Assert.assertEquals(1, classificationToTags.get("test_classification").size());
    Assert.assertEquals("test_tag_1", classificationToTags.get("test_classification").get(0));
    // Tag.ColumnTags
    Map<String, Map<String, List<String>>> columnToClassificationToTags = tagInfo.getColumnTags();
    Assert.assertNotNull(columnToClassificationToTags);
    Assert.assertEquals(2, columnToClassificationToTags.size());
    // Tag.ColumnTags.a
    Assert.assertTrue(columnToClassificationToTags.containsKey("a"));
    Assert.assertEquals(1, columnToClassificationToTags.get("a").size());
    Assert.assertNotNull(columnToClassificationToTags.get("a").get("test_classification"));
    Assert.assertEquals(1, columnToClassificationToTags.get("a").get("test_classification").size());
    Assert.assertEquals("test_tag_1", columnToClassificationToTags.get("a").get("test_classification").get(0));
    // Tag.Columntags.b
    Assert.assertTrue(columnToClassificationToTags.containsKey("b"));
    Assert.assertEquals(1, columnToClassificationToTags.get("b").size());
    Assert.assertNotNull(columnToClassificationToTags.get("b").get("test_classification"));
    Assert.assertEquals(1, columnToClassificationToTags.get("b").get("test_classification").size());
    Assert.assertEquals("test_tag_1", columnToClassificationToTags.get("b").get("test_classification").get(0));
    // Tag.UpdateTime
    Assert.assertEquals(Long.valueOf(1607483676), tagInfo.getUpdateTime());
  }
}
