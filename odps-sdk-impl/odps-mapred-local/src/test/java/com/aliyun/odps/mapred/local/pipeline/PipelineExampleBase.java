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

package com.aliyun.odps.mapred.local.pipeline;

import org.junit.Before;

import com.aliyun.odps.mapred.local.utils.TestUtils;


public class PipelineExampleBase extends PipeLine {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

  public static String getInputTableName(Object object) {
    String result = object.getClass().getName();
    result = result.substring(result.lastIndexOf(".") + 1);
    return result.toLowerCase() + "_in";
  }

  public static String getOutputTableName(Object object) {
    String result = object.getClass().getName();
    result = result.substring(result.lastIndexOf(".") + 1);
    return result.toLowerCase() + "_out";
  }
}
