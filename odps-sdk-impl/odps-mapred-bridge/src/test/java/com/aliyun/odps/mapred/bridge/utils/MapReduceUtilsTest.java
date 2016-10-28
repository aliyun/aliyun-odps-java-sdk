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

package com.aliyun.odps.mapred.bridge.utils;

import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.example.WordCount;

public class MapReduceUtilsTest {

  @Test
  public void testGetMethod() {
    Method m = MapReduceUtils.getOverriddenMethod(WordCount.TokenizerMapper.class, "map",
                                                  long.class, Record.class,
                                                  Mapper.TaskContext.class);
    Assert.assertNotNull(m);
  }

  class OverridenClass extends WordCount.TokenizerMapper {

  }

  @Test
  public void testGetOverriddenMethod() {
    Method m = MapReduceUtils.getOverriddenMethod(OverridenClass.class, "map", long.class,
                                                  Record.class, Mapper.TaskContext.class);
    Assert.assertNotNull(m);
  }

  class EmptyClass {

  }

  @Test
  public void testNonExistMethod() {
    Method m = MapReduceUtils.getOverriddenMethod(EmptyClass.class, "map", long.class,
                                                  Record.class, Mapper.TaskContext.class);
    Assert.assertNull(m);
  }

  @Test
  public void testParameterTypeNotMatch() {
    Method m = MapReduceUtils
        .getOverriddenMethod(EmptyClass.class, "map", long.class, Record.class);
    Assert.assertNull(m);
  }

  @Test
  public void testCompatibility() {
    Method m = MapReduceUtils.getOverriddenMethod(WordCount.TokenizerMapper.class, "map",
                                                  long.class, Record.class,
                                                  com.aliyun.odps.mapred.TaskContext.class);
    Assert.assertNotNull(m);
  }

  @Test
  public void testPartSpecInclusive() {
    Assert.assertTrue(MapReduceUtils.partSpecInclusive(
        TableInfo.builder().projectName("foo").tableName("bar").build(),
        TableInfo.builder().projectName("foo").tableName("bar").build()));

    Assert.assertTrue(MapReduceUtils.partSpecInclusive(
        TableInfo.builder().projectName("foo").tableName("bar").build(),
        TableInfo.builder().projectName("foo").tableName("bar").partSpec("foo=1").build()));

    Assert.assertTrue(MapReduceUtils.partSpecInclusive(
        TableInfo.builder().projectName("foo").tableName("bar").partSpec("foo=1").build(),
        TableInfo.builder().projectName("foo").tableName("bar").partSpec("foo=1/bar=2").build()));

    Assert.assertFalse(MapReduceUtils.partSpecInclusive(
        TableInfo.builder().projectName("foo").tableName("bar").partSpec("foo=1/bar=2").build(),
        TableInfo.builder().projectName("foo").tableName("bar").partSpec("foo=1").build()));
  }
}
