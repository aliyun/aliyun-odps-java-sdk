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

package com.aliyun.odps.table.impl.v2;

import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Runs the same data-plane contracts after real Java task transport. */
public class V2SerializedWorkerContractTest extends V2CompatibleContractTest {
  private Object transport(Object session) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(session);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return in.readObject();
    }
  }

  @Override
  protected TableBatchReadSession transferRead(TableBatchReadSession session) throws Exception {
    return (TableBatchReadSession) transport(session);
  }

  @Override
  protected TableBatchWriteSession transferWrite(TableBatchWriteSession session) throws Exception {
    return (TableBatchWriteSession) transport(session);
  }
}
