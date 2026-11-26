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

package com.aliyun.odps.storage.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Public descriptor parsing requires neither table-api nor a live storage client. */
class SessionDescriptorTest {
  private static final String READ =
      "{\"version\":1,\"provider\":\"v2\",\"kind\":\"batch-read\","
          + "\"table\":{\"project\":\"p\",\"schema\":\"default\",\"table\":\"t\"},"
          + "\"metadata\":{\"sessionId\":\"r1\",\"sessionStatus\":\"NORMAL\","
          + "\"dataSchema\":{\"dataColumns\":[{\"name\":\"a\",\"type\":\"bigint\"}]},"
          + "\"splitMode\":\"RowOffset\",\"recordCount\":9},"
          + "\"readOptions\":{\"rowsPerSplit\":4}}";

  @Test
  void fixedVersionOneDescriptionIsIndependentOfClientAndJavaClassNames() {
    SessionDescriptor descriptor = SessionDescriptor.fromJson(READ);
    assertEquals(SessionDescriptor.Kind.BATCH_READ, descriptor.getKind());
    assertEquals("r1", descriptor.getSessionId());
    assertEquals("t", descriptor.getTableIdentifier().getTable());
    assertEquals(READ, descriptor.toJson());
    assertThrows(IllegalStateException.class, descriptor::getArrowOptions);
  }

  @Test
  void invalidPlanningNumbersAreRejectedBeforeBinding() {
    for (String count : new String[] {"-1", "1.5", "\"9\"", "9223372036854775808"}) {
      String invalid = READ.replace("\"recordCount\":9", "\"recordCount\":" + count);
      assertThrows(IllegalArgumentException.class, () -> SessionDescriptor.fromJson(invalid));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> SessionDescriptor.fromJson(READ.replace("\"rowsPerSplit\":4", "\"rowsPerSplit\":0")));
  }

  @Test
  void descriptorVersionIsExplicitAndUnsupportedKindsFailOffline() {
    for (String invalid :
        new String[] {
          READ.replace("\"version\":1", "\"version\":2"),
          READ.replace("\"version\":1", "\"version\":\"1\""),
          READ.replace("batch-read", "streaming-write"),
          "null",
          "[]"
        }) {
      assertThrows(IllegalArgumentException.class, () -> SessionDescriptor.fromJson(invalid));
    }
  }
}
