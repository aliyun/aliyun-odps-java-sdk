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

package com.aliyun.odps.graph;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.aliyun.odps.data.TableInfo;

public class JobConfTest {

  @Test
  public void testStrip() throws IOException {
    
    JobConf conf = new JobConf();
    
    TableInfo t = TableInfo.builder().tableName("bbb").partSpec("ds='20140511'").build();
    conf.addInput(t);
    
    String input = conf.get(GRAPH_CONF.INPUT_DESC);
    String jsonStr = "[{\"projName\":\"\",\"tblName\":\"bbb\",\"partSpec\":[\"ds\\u003d20140511\"],\"cols\":\"\"}]";
    assertEquals(jsonStr, input);
  }
}
