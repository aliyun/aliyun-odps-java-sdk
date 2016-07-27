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

package com.aliyun.odps.graph.local.message;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.graph.local.RuntimeContext;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;

public class MsgManager {

  private static Log LOG = LogFactory.getLog(MsgManager.class);

  public MsgManager() {
    curBuffer = new SuperStepBuffer(-1);
    nextBuffer = new SuperStepBuffer(0);
  }

  private SuperStepBuffer curBuffer = null;
  private SuperStepBuffer nextBuffer = null;

  public void pushMsg(RuntimeContext context, long superStep,
                      WritableComparable<?> vertexId, Writable msg) {
    if (superStep != nextBuffer.getSuperStep()) {
      throw new RuntimeException(
          "Internal error, super step is inconsistent, expected "
          + nextBuffer.getSuperStep() + ", but " + superStep);
    }
    nextBuffer.pushMsg(vertexId, msg);
  }

  public boolean hasMessageForVertex(RuntimeContext context, long superStep,
                                     WritableComparable<?> vertexId) {
    if (superStep != curBuffer.getSuperStep()) {
      throw new RuntimeException(
          "Internal error, super step is inconsistent, expected "
          + curBuffer.getSuperStep() + ", but " + superStep);
    }
    return curBuffer.hasMsg(vertexId);
  }

  public Iterable<Writable> popMsges(RuntimeContext context, long superStep,
                                     WritableComparable<?> vertexId) {
    if (superStep != curBuffer.getSuperStep()) {
      throw new RuntimeException(
          "Internal error, super step is inconsistent, expected "
          + curBuffer.getSuperStep() + ", but " + superStep);
    }
    return curBuffer.popMsges(vertexId);
  }

  public void nextSuperStep(RuntimeContext context)
      throws IOException {
    // dump next buffer, from 0
    nextBuffer.dump(context);

    curBuffer = nextBuffer;
    nextBuffer = new SuperStepBuffer(curBuffer.getSuperStep() + 1);
  }

  public Set<WritableComparable<?>> getVertexIDList() {
    return curBuffer.getVertexIDList();
  }

  public boolean hasNextStepMessages() {
    return nextBuffer.hasMessages();
  }

}
