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

package com.aliyun.odps.mapred.bridge;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.mapred.bridge.utils.VersionUtils;

public class VersionUtilsTest {

  @Test
  public void testAdaptRecord() throws IOException {
    com.aliyun.odps.Record r =
        TestUtils.mockOldRecord(new OdpsType[]{OdpsType.STRING}, new String[]{"word"},
                                new Writable[]{new Text("hello")});
    Record nr = VersionUtils.adaptRecord(r);
    assertEquals("hello", nr.get(0));
    assertEquals("hello", nr.get("word"));
    assertEquals("hello", nr.getString(0));
  }
}
