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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Classification.AttributeDefinition;
import com.aliyun.odps.Classification.BooleanAttributeDefinition;
import com.aliyun.odps.Classification.EnumAttributeDefinition;
import com.aliyun.odps.Classification.IntegerAttributeDefinition;
import com.aliyun.odps.Classification.StringAttributeDefinition;
import com.aliyun.odps.commons.transport.OdpsTestUtils;

public class ClassificationsTest extends TestBase {

  private static final String BASE_CLASSIFICATION_NAME_PREFIX =
      ClassificationsTest.class.getSimpleName();

  private static final List<String> classificationsToDrop = new LinkedList<>();

  @AfterClass
  public static void tearDown() {
    for (String name : classificationsToDrop) {
      try {
        odps.classifications().delete(name);
      } catch (OdpsException e) {
        System.err.println("Failed to drop classification: " + name);
      }
    }

    for (Classification classification : odps.classifications()) {
      if (classification.getName().startsWith(BASE_CLASSIFICATION_NAME_PREFIX)) {
        try {
          odps.classifications().delete(classification.getName());
        } catch (OdpsException e) {
          System.err.println("Failed to drop classification: " + classification.getName());
        }
      }
    }
  }

  @Test
  public void testCreateClassification() throws OdpsException {
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
    String name = String.format(
        "%s_%s_%s",
        BASE_CLASSIFICATION_NAME_PREFIX,
        "testCreateClassification",
        OdpsTestUtils.getRandomName());
    odps.classifications().create(name, attributes, true);
    classificationsToDrop.add(name);

    Classification classification = odps.classifications().get(name);
    Assert.assertEquals(odps.getDefaultProject(), classification.getProject());
    Assert.assertEquals(name, classification.getName());
    Assert.assertEquals(OdpsTestUtils.getCurrentUser(odps), classification.getOwner());
    Assert.assertEquals(4, classification.getAttributeDefinitions().size());

    AttributeDefinition strAttr = classification.getAttributeDefinitions().get("str_attr");
    Assert.assertFalse(strAttr.isRequired());
    // TODO: server side unsupported
//    Assert.assertEquals("yyyy-MM-dd", ((StringAttributeDefinition) strAttr).getPattern());
    Assert.assertEquals(Integer.valueOf(10), ((StringAttributeDefinition) strAttr).getMaxLength());
    Assert.assertEquals(Integer.valueOf(10), ((StringAttributeDefinition) strAttr).getMinLength());

    AttributeDefinition intAttr = classification.getAttributeDefinitions().get("int_attr");
    Assert.assertFalse(intAttr.isRequired());
    Assert.assertEquals(Integer.valueOf(10), ((IntegerAttributeDefinition) intAttr).getMaximum());
    Assert.assertEquals(Integer.valueOf(0), ((IntegerAttributeDefinition) intAttr).getMinimum());

    AttributeDefinition enumAttr = classification.getAttributeDefinitions().get("enum_attr");
    Set<String> elements = new HashSet<>();
    elements.add("foo");
    elements.add("bar");
    Assert.assertEquals(elements, ((EnumAttributeDefinition) enumAttr).getElements());

    AttributeDefinition boolAttr = classification.getAttributeDefinitions().get("bool_attr");
    Assert.assertTrue(boolAttr instanceof BooleanAttributeDefinition);
  }

  @Test
  public void testDeleteClassification() throws OdpsException {
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
    String name = String.format(
        "%s_%s_%s",
        BASE_CLASSIFICATION_NAME_PREFIX,
        "testDeleteClassification",
        OdpsTestUtils.getRandomName());
    odps.classifications().create(name, attributes, true);

    odps.classifications().delete(name);
    Assert.assertFalse(odps.classifications().exists(name));
  }

  @Test
  public void testUpdateClassification() throws OdpsException {
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
    String name = String.format(
        "%s_%s_%s",
        BASE_CLASSIFICATION_NAME_PREFIX,
        "testUpdateClassification",
        OdpsTestUtils.getRandomName());
    odps.classifications().create(name, attributes, true);

    Classification classification = odps.classifications().get(name);

    // Add attribute
    classification.addAttributeDefinition(
        "str_attr2", new StringAttributeDefinition.Builder().build());
    classification.reload();
    Assert.assertTrue(classification.getAttributeDefinitions().containsKey("str_attr2"));

    classification.reload();
    Assert.assertTrue(classification.getAttributeDefinitions().containsKey("bool_attr"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetNonexistentClassification() throws OdpsException {
    String name = String.format(
        "%s_%s_%s",
        BASE_CLASSIFICATION_NAME_PREFIX,
        "testGetNonexistentClassification",
        OdpsTestUtils.getRandomName());
    odps.classifications().get(name).reload();
  }

  @Test
  public void testClassificationIterator() throws OdpsException {
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
    String name = String.format(
        "%s_%s_%s",
        BASE_CLASSIFICATION_NAME_PREFIX,
        "testClassificationIterator",
        OdpsTestUtils.getRandomName());
    odps.classifications().create(name, attributes, true);

    boolean found = false;
    for (Classification classification : odps.classifications()) {
      if (name.equals(classification.getName())) {
        found = true;
      }
    }

    Assert.assertTrue(found);
  }

   // TODO: test classification page splits
}
