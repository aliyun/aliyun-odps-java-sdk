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

package com.aliyun.odps.local.common;

import java.io.File;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.conf.Configuration;

public class JobDirecotryTest {

  static WareHouse wareHouse = WareHouse.getInstance();

  @BeforeClass
  public static void setUpBeforeClass() {
    Configuration configuration = new Configuration();
    configuration.set(Constants.JOB_NAME, "mr_20150212083126_505_21462");
    wareHouse.setConfiguration(configuration);
  }

  @Test
  public void testConvertInputFile() {
    asertTrue("temp/mr_20150212083126_505_21462/input");
    asertTrue("temp/mr_20150212083126_505_21462/input/");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name/");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name/wc_in2");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name/wc_in2/");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name/wc_in2/p1=1");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name/wc_in2/p1=1/");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name/wc_in2/p1=1/p2=2");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name/wc_in2/p1=1/p2=2/");
    asertTrue("temp/mr_20150212083126_505_21462/input/project_name/wc_in2/p1=1/p2=2/data");

  }

  private void asertTrue(String inpuFileStr) {
    File inputFile = new File(inpuFileStr);
    Assert.assertTrue(inputFile.exists());
    File whInputFile = wareHouse.getJobDir().convertInputFile(inputFile);
    Assert.assertTrue(whInputFile.exists());
  }

}
