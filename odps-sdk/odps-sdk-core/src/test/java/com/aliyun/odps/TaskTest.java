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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.task.GraphTask;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Test;

import com.aliyun.odps.task.SQLTask;

public class TaskTest {
  private String sqlTaskXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<SQL>\n"
      + "   <Name>name</Name>\n"
      + "   <Config>\n"
      + "      <Property>\n"
      + "         <Name>a</Name>\n"
      + "         <Value>b</Value>\n"
      + "      </Property>\n"
      + "      <Property>\n"
      + "         <Name>c</Name>\n"
      + "         <Value>d</Value>\n"
      + "      </Property>\n"
      + "   </Config>\n"
      + "   <Query>select count(*) from src;</Query>\n"
      + "</SQL>";

  @Test
  public void testGetCommandText() {
    SQLTask task = new SQLTask();
    task.setName("name");
    task.setQuery("select count(*) from src;");
    assertEquals(task.getCommandText(), "select count(*) from src;");
  }

  @Test
  public void testMarshalSQLTask() throws Exception {
    SQLTask task = new SQLTask();
    task.setName("name");
    task.setQuery("select count(*) from src;");
    task.setProperty("a", "b");
    task.setProperty("c", "d");
    String st = SimpleXmlUtils.marshal(task);

    assertEquals(sqlTaskXml, st);
  }

  @Test
  public void testUnmarshalSQLTask() throws Exception {
    SQLTask sqlTask = SimpleXmlUtils.unmarshal(sqlTaskXml.getBytes(), SQLTask.class);

    System.out.println("Name: " + sqlTask.getName());
    System.out.println("Query: " + sqlTask.getQuery());
    System.out.println("Properties: " + sqlTask.getProperties());

    assertEquals("select count(*) from src;", sqlTask.getQuery());
    assertEquals("name", sqlTask.getName());
    assertTrue(sqlTask.getProperties().containsKey("a"));
    assertEquals("b", sqlTask.getProperties().get("a"));
    assertTrue(sqlTask.getProperties().containsKey("c"));
    assertEquals("d", sqlTask.getProperties().get("c"));
  }

  @Test
  public void testDefaultSettings() {
    Task task = new SQLTask();
    task.loadDefaultSettings();
    assertTrue(task.getProperties().containsKey("settings"));
  }

  @Test
  public void testMergeSettingsWithDefault() {
    String userDefinedValue = "user defined value";
    String[] defaultSettings = {"odps.idata.userenv"};

    Task task = new SQLTask();
    JsonObject userSettings = new JsonObject();
    for (String setting : defaultSettings) {
      userSettings.addProperty(setting, userDefinedValue);
    }
    task.setProperty("settings", userSettings.toString());

    task.loadDefaultSettings();
    JsonParser parser = new JsonParser();
    JsonObject settings = parser.parse(task.getProperties().get("settings")).getAsJsonObject();
    for (String setting : defaultSettings) {
      assertNotEquals(userDefinedValue, settings.get(setting));
    }
  }
}
