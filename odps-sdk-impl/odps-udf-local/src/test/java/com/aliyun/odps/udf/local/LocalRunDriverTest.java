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

package com.aliyun.odps.udf.local;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.udf.UDFException;

public class LocalRunDriverTest {

  Main driver;

  @Before
  public void setUp() throws Exception {
    driver = new Main();
  }

  @Test
  public void testUdtf() throws ParseException, LocalRunException, UDFException, IOException {
    String[] args = new String[]{"-c", "com.aliyun.odps.udf.local.examples.Udtf_ss2si", "-i",
                                 "wc_in1.c(col2,col3)", "-p", "project_name", "--endpoint", "xxx",
                                 "--access-id", "xxx",
                                 "--access-key", "xxx"};
    driver.main(args);
  }

  @Test
  public void testUdf() throws ParseException, LocalRunException, UDFException, IOException {
    String[] args = new String[]{"-c", "com.aliyun.odps.udf.local.examples.UdfExample", "-i",
                                 "wc_in1.c(col2,col3)", "-p", "project_name", "--endpoint", "xxx",
                                 "--access-id", "xxx",
                                 "--access-key", "xxx"};
    driver.main(args);

    args = new String[]{"-c", "com.aliyun.odps.udf.local.examples.UdfExample", "-i", "wc_in2",
                        "-p", "project_name", "--endpoint", "xxx", "--access-id", "xxx",
                        "--access-key", "xxx"};
    driver.main(args);
  }

  @Test
  public void testUdaf() throws ParseException, LocalRunException, UDFException, IOException {
    String[] args = new String[]{"-c", "com.aliyun.odps.udf.local.examples.AggregateCharCount",
                                 "-i", "wc_in1.c(col2,col3)", "-p", "project_name", "--endpoint",
                                 "xxx", "--access-id",
                                 "xxx", "--access-key", "xxx"};
    driver.main(args);
  }

}
