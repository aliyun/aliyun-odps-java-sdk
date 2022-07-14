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

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Classification.AttributeDefinition;
import com.aliyun.odps.Classification.AttributeDeserializer;
import com.aliyun.odps.Classification.AttributeSerializer;
import com.aliyun.odps.Classification.BooleanAttributeDefinition;
import com.aliyun.odps.Classification.ClassificationModel;
import com.aliyun.odps.Classification.ClassificationModelDeserializer;
import com.aliyun.odps.Classification.ClassificationModelSerializer;
import com.aliyun.odps.Classification.EnumAttributeDefinition;
import com.aliyun.odps.Classification.IntegerAttributeDefinition;
import com.aliyun.odps.Classification.StringAttributeDefinition;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ClassificationTest {

  @Test
  public void testAttributeSerializer() {
    Gson gson = new GsonBuilder()
        .registerTypeHierarchyAdapter(AttributeDefinition.class, new AttributeSerializer())
        .create();

    AttributeDefinition strAttr = new StringAttributeDefinition.Builder()
        .maxLength(10)
        .minLength(10)
        .build();
    String expected = "{\"Type\":\"string\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"MaxLength\":10,\"MinLength\":10}}";
    Assert.assertEquals(expected, gson.toJson(strAttr));

    AttributeDefinition intAttr = new IntegerAttributeDefinition.Builder()
        .minimum(0)
        .maximum(10)
        .build();
    expected = "{\"Type\":\"integer\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"Maximum\":10,\"Minimum\":0}}";
    Assert.assertEquals(expected, gson.toJson(intAttr));

    AttributeDefinition enumAttr = new EnumAttributeDefinition.Builder()
        .element("foo")
        .element("bar")
        .build();
    expected = "{\"Type\":\"enum\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"Elements\":[\"bar\",\"foo\"]}}";
    Assert.assertEquals(expected, gson.toJson(enumAttr));

    AttributeDefinition boolAttr = new BooleanAttributeDefinition.Builder().build();
    expected = "{\"Type\":\"boolean\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true}}";
    Assert.assertEquals(expected, gson.toJson(boolAttr));
  }

  @Test
  public void testAttributeDeserializer() {
    Gson gson = new GsonBuilder()
        .registerTypeHierarchyAdapter(AttributeDefinition.class, new AttributeDeserializer())
        .create();

    String json = "{\"Type\":\"string\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"MaxLength\":10,\"MinLength\":10}}";
    AttributeDefinition strAttr = gson.fromJson(json, AttributeDefinition.class);
    Assert.assertFalse(strAttr.isRequired());
    Assert.assertEquals(Integer.valueOf(10), ((StringAttributeDefinition) strAttr).getMaxLength());
    Assert.assertEquals(Integer.valueOf(10), ((StringAttributeDefinition) strAttr).getMinLength());

    json = "{\"Type\":\"integer\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"Maximum\":10,\"Minimum\":0}}";
    AttributeDefinition intAttr = gson.fromJson(json, AttributeDefinition.class);
    Assert.assertFalse(intAttr.isRequired());
    Assert.assertEquals(Integer.valueOf(10), ((IntegerAttributeDefinition) intAttr).getMaximum());
    Assert.assertEquals(Integer.valueOf(0), ((IntegerAttributeDefinition) intAttr).getMinimum());

    json = "{\"Type\":\"enum\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"Elements\":[\"bar\",\"foo\"]}}";
    AttributeDefinition enumAttr = gson.fromJson(json, AttributeDefinition.class);
    Set<String> elements = new HashSet<>();
    elements.add("foo");
    elements.add("bar");
    Assert.assertEquals(elements, ((EnumAttributeDefinition) enumAttr).getElements());

    json = "{\"Type\":\"boolean\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true}}";
    AttributeDefinition boolAttr = gson.fromJson(json, AttributeDefinition.class);
    Assert.assertTrue(boolAttr instanceof BooleanAttributeDefinition);
  }

  @Test
  public void testClassificationModelSerializer() {
    Gson gson = new GsonBuilder()
        .registerTypeHierarchyAdapter(AttributeDefinition.class, new AttributeSerializer())
        .registerTypeAdapter(ClassificationModel.class, new ClassificationModelSerializer())
        .create();

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

    ClassificationModel model = new ClassificationModel();
    Date current = new Date();
    model.project = "test_project";
    model.name = "test_classification";
    model.owner = "ALIYUN$odpstest1@aliyun.com";
    model.createdTime = current;
    model.lastModifiedTime = current;
    model.attributes.putAll(attributes);

    System.out.println(gson.toJson(model, model.getClass()));
  }

  @Test
  public void testClassificationModelDeserializer() {
    Gson gson = new GsonBuilder()
        .registerTypeHierarchyAdapter(AttributeDefinition.class, new AttributeDeserializer())
        .registerTypeAdapter(ClassificationModel.class, new ClassificationModelDeserializer())
        .create();

    String json = "{\"Attributes\":{\"int_attr\":{\"Type\":\"integer\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"Maximum\":10,\"Minimum\":0}},\"bool_attr\":{\"Type\":\"boolean\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true}},\"str_attr\":{\"Type\":\"string\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"MaxLength\":10,\"MinLength\":10}},\"enum_attr\":{\"Type\":\"enum\",\"Constraints\":{\"IsRequired\":false,\"Unique\":true,\"Elements\":[\"bar\",\"foo\"]}}},\"CreateTime\":1606725399,\"DatabaseName\":\"test_project\",\"Id\":\"37fc0d1e36fb4a7ea5e6e4f13261855f\",\"Name\":\"test_classification\",\"Owner\":\"ALIYUN$odpstest1@aliyun.com\",\"UpdateTime\":1606796484}";
    ClassificationModel model = gson.fromJson(json, ClassificationModel.class);
    Assert.assertEquals("test_project", model.project);
    Assert.assertEquals("test_classification", model.name);
    Assert.assertEquals("ALIYUN$odpstest1@aliyun.com", model.owner);
    Assert.assertEquals(1606725399, model.createdTime.getTime());
    Assert.assertEquals(1606796484, model.lastModifiedTime.getTime());
    Assert.assertEquals(4, model.attributes.size());

    AttributeDefinition strAttr = model.attributes.get("str_attr");
    Assert.assertFalse(strAttr.isRequired());
    Assert.assertEquals(Integer.valueOf(10), ((StringAttributeDefinition) strAttr).getMaxLength());
    Assert.assertEquals(Integer.valueOf(10), ((StringAttributeDefinition) strAttr).getMinLength());

    AttributeDefinition intAttr = model.attributes.get("int_attr");
    Assert.assertFalse(intAttr.isRequired());
    Assert.assertEquals(Integer.valueOf(10), ((IntegerAttributeDefinition) intAttr).getMaximum());
    Assert.assertEquals(Integer.valueOf(0), ((IntegerAttributeDefinition) intAttr).getMinimum());

    AttributeDefinition enumAttr = model.attributes.get("enum_attr");
    Set<String> elements = new HashSet<>();
    elements.add("foo");
    elements.add("bar");
    Assert.assertEquals(elements, ((EnumAttributeDefinition) enumAttr).getElements());

    AttributeDefinition boolAttr = model.attributes.get("bool_attr");
    Assert.assertTrue(boolAttr instanceof BooleanAttributeDefinition);
  }
}
