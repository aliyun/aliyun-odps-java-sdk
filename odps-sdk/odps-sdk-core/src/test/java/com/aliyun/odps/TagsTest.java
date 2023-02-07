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


package com.aliyun.odps;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Classification.AttributeDefinition;
import com.aliyun.odps.Classification.BooleanAttributeDefinition;
import com.aliyun.odps.Classification.EnumAttributeDefinition;
import com.aliyun.odps.Classification.IntegerAttributeDefinition;
import com.aliyun.odps.Classification.StringAttributeDefinition;
import com.aliyun.odps.Tags.TagBuilder;
import com.aliyun.odps.commons.transport.OdpsTestUtils;

public class TagsTest extends TestBase {
  private static final String BASE_TAG_NAME_PREFIX = TagsTest.class.getSimpleName();
  private static final String BASE_CLASSIFICATION_NAME_PREFIX = TagsTest.class.getSimpleName();

  private static final List<String> classificationsToDrop = new LinkedList<>();

  @AfterClass
  public static void tearDown() {
    // TODO: tag deletion is not supported yet
//    for (String classification : classificationsToDrop) {
//      try {
//        Classification c = odps.classifications().get(classification);
//        for (Tag tag : c.tags()) {
//          c.tags().delete(tag.getName());
//        }
//        odps.classifications().delete(classification);
//      } catch (OdpsException e) {
//        e.printStackTrace();
//        System.out.println("Failed to drop classification: " + classification);
//      }
//    }
  }

  @Test
  public void testCreateTag() throws OdpsException {
    Classification classification = createClassification();
    String name = String.format(
        "%s_%s_%s",
        BASE_TAG_NAME_PREFIX,
        "testCreateTag",
        OdpsTestUtils.getRandomName());
    TagBuilder tagBuilder = new TagBuilder(classification, name);
    tagBuilder.attribute("str_attr", "1234567890");
    tagBuilder.attribute("int_attr", "5");
    tagBuilder.attribute("enum_attr", "foo");
    tagBuilder.attribute("bool_attr", "true");
    classification.tags().create(tagBuilder, true);
  }

  @Test
  public void testUpdateTag() throws OdpsException {
    Classification classification = createClassification();
    String name = String.format(
        "%s_%s_%s",
        BASE_TAG_NAME_PREFIX,
        "testUpdateTag",
        OdpsTestUtils.getRandomName());
    TagBuilder tagBuilder = new TagBuilder(classification, name);
    tagBuilder.attribute("str_attr", "1234567890");
    tagBuilder.attribute("enum_attr", "foo");
    tagBuilder.attribute("bool_attr", "true");
    classification.tags().create(tagBuilder, true);

    Tag tag = classification.tags().get(name);

    // Update value
    tag.setAttribute("str_attr", "9876543210");
    classification.tags().update(tag);
    tag.reload();
    Assert.assertEquals("9876543210", tag.getAttributes().get("str_attr"));

    // Add attribute
    tag.setAttribute("int_attr", "5");
    classification.tags().update(tag);
    tag.reload();
    Assert.assertEquals("5", tag.getAttributes().get("int_attr"));

    // Remove attribute
    tag.removeAttribute("bool_attr");
    classification.tags().update(tag);
    tag.reload();
    Assert.assertFalse(tag.getAttributes().containsKey("bool_attr"));
  }

  @Test
  public void testTagIterator() throws OdpsException {
    Classification classification = createClassification();
    List<String> tags = new LinkedList<>();

    String name = String.format(
        "%s_%s_%s", BASE_TAG_NAME_PREFIX, "testTagIterator", OdpsTestUtils.getRandomName());
    TagBuilder tagBuilder = new TagBuilder(classification, name);
    tagBuilder.attribute("str_attr", "1234567890");
    tagBuilder.attribute("int_attr", "5");
    tagBuilder.attribute("enum_attr", "foo");
    tagBuilder.attribute("bool_attr", "true");
    classification.tags().create(tagBuilder, true);
    tags.add(name);

    name = String.format(
        "%s_%s_%s", BASE_TAG_NAME_PREFIX, "testTagIterator", OdpsTestUtils.getRandomName());
    tagBuilder = new TagBuilder(classification, name);
    tagBuilder.attribute("str_attr", "1234567890");
    tagBuilder.attribute("int_attr", "5");
    tagBuilder.attribute("enum_attr", "foo");
    tagBuilder.attribute("bool_attr", "true");
    classification.tags().create(tagBuilder, true);
    tags.add(name);

    name = String.format(
        "%s_%s_%s", BASE_TAG_NAME_PREFIX, "testTagIterator", OdpsTestUtils.getRandomName());
    tagBuilder = new TagBuilder(classification, name);
    tagBuilder.attribute("str_attr", "1234567890");
    tagBuilder.attribute("int_attr", "5");
    tagBuilder.attribute("enum_attr", "foo");
    tagBuilder.attribute("bool_attr", "true");
    classification.tags().create(tagBuilder, true);
    tags.add(name);

    for (String tagName : tags) {
      classification.tags().get(tagName).reload();
    }

    int count = 0;
    for (Tag tag : classification.tags()) {
      count += 1;
    }
    Assert.assertEquals(tags.size(), count);
  }

  private Classification createClassification() throws OdpsException {
    Map<String, AttributeDefinition> attributes = new HashMap<>();
    attributes.put("str_attr", new StringAttributeDefinition.Builder().maxLength(10)
                                                                      .minLength(10)
                                                                      .build());
    attributes.put("int_attr", new IntegerAttributeDefinition.Builder().minimum(0)
                                                                       .maximum(10)
                                                                       .build());
    attributes.put("enum_attr", new EnumAttributeDefinition.Builder().element("foo")
                                                                     .element("bar")
                                                                     .build());
    attributes.put("bool_attr", new BooleanAttributeDefinition.Builder().build());
    String classificationName = String.format(
        "%s_%s",
        BASE_CLASSIFICATION_NAME_PREFIX,
        OdpsTestUtils.getRandomName());
    odps.classifications().create(classificationName, attributes, true);
    classificationsToDrop.add(classificationName);

    return odps.classifications().get(classificationName);
  }
}
