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

package com.aliyun.testodps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.OdpsDeprecatedLogger;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class OdpsDeprecatedLoggerTest {
  final static ArrayList<String> keyPutted = new ArrayList<String>();

  @BeforeClass
  public static void setup() {
    final ConcurrentHashMap<String, Long> calls = OdpsDeprecatedLogger.getDeprecatedCalls();

    ConcurrentHashMap<String, Long> mockCalls = new ConcurrentHashMap<String, Long>() {
      @Override
      public int size() {
        return calls.size();
      }

      @Override
      public boolean isEmpty() {
        return super.isEmpty();
      }

      @Override
      public Long put(String key, Long value) {
        keyPutted.add(key);
        return calls.put(key, value);
      }

      @Override
      public void clear() {
        super.clear();
      }
    };

    OdpsDeprecatedLogger.setDeprecatedCalls(mockCalls);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDeprecatedLogger() throws IOException {
    TestAnnotation.testDeprecated();
    for (String key : keyPutted) {
      if (key.contains("testDeprecated")) {
        return;
      }
    }

    throw new IOException("Keys not contains testDeprecated.");
  }

  @Test
  public void testOdpsImplLogger() throws IOException {
    TestAnnotation.testImpl();
    for (String key : keyPutted) {
      if (key.contains("testImpl")) {
        return;
      }
    }

    throw new IOException("Keys not contains testImpl.");
  }
}
