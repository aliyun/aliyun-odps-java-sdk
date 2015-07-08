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

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlSeeAlso;

import org.junit.Test;

import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.task.SQLTask;

public class TaskTest {

  @Test
  public void testGetCommandText() {
    SQLTask task = new SQLTask();
    task.setName("name");
    task.setQuery("select count(*) from src;");
    assertEquals(task.getCommandText(), "select count(*) from src;");
  }

  @Test
  public void testMarshalTask() throws JAXBException {
    SQLTask task = new SQLTask();
    task.setName("name");
    task.setQuery("select count(*) from src;");
    task.setProperty("a", "b");
    String st = JAXBUtils.marshal(task, Task.class);
    assertEquals(
        "<?xml version=\"1.0\" ?><SQL><Name>name</Name><Config><Property><Name>a</Name><Value>b</Value></Property></Config><Query>select count(*) from src;</Query></SQL>",
        st);
  }

  @Test
  public void testInternalTaskInject() throws JAXBException {
    XmlSeeAlso anotation = Task.class.getAnnotation(XmlSeeAlso.class);
    Class<?>[] value = anotation.value();
    assertEquals(false,
                 "com.aliyun.odps.task.InternalTask".equals(value[value.length - 1].getName()));
  }
}
