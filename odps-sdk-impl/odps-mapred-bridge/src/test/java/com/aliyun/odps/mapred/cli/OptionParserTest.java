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

package com.aliyun.odps.mapred.cli;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.aliyun.odps.mapred.conf.SessionState;

public class OptionParserTest {

  @Test
  public void testParse() throws Exception {
    SessionState ss = SessionState.get();
    OptionParser parser = new OptionParser(ss);
    parser
        .parse(new String[]{"-resources", "jar1.jar,jar2.jar", "-odpsconf",
                            "odps.access.id=accessId", "-odpsconf", "odps.access.key=accessKey",
                            "-odpsconf",
                            "odps.account.provider=ALIYUN",
                            "com.aliyun.odps.mapred.example.WordCount", "tin",
                            "tout"});
    assertArrayEquals(new String[]{"jar1.jar", "jar2.jar"}, ss.getDefaultJob().getResources());
    assertEquals("com.aliyun.odps.mapred.example.WordCount", parser.getMainClass().getName());
    assertArrayEquals(new String[]{"tin", "tout"}, parser.getArguments());
  }


}
