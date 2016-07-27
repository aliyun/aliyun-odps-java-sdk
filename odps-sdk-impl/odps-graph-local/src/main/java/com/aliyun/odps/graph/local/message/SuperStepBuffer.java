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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.aliyun.odps.graph.local.RuntimeContext;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;

public class SuperStepBuffer {

  private long superStep;
  private Map<WritableComparable<?>, MsgBuffer> msgBuffer = null;

  public SuperStepBuffer(long superstep) {
    superStep = superstep;
    msgBuffer = new HashMap<WritableComparable<?>, MsgBuffer>();
  }

  public void setSuperStep(long superstep) {
    this.superStep = superstep;
  }

  public long getSuperStep() {
    return this.superStep;
  }

  public void pushMsg(WritableComparable<?> vertexId, Writable msg) {
    if (msgBuffer.containsKey(vertexId)) {
      msgBuffer.get(vertexId).addMessage(msg);
    } else {
      MsgBuffer buffer = new MsgBuffer();
      buffer.addMessage(msg);
      msgBuffer.put(vertexId, buffer);
    }
  }

  public boolean hasMsg(WritableComparable<?> vertexId) {
    return msgBuffer.containsKey(vertexId)
           && msgBuffer.get(vertexId).hasMessages();
  }

  public Iterable<Writable> popMsges(final WritableComparable<?> vertexId) {
    return new MsgIterable(vertexId);
  }

  public void dump(RuntimeContext context) throws IOException {
    File superStepDir = new File(context.getSuperStepDir(),
                                 String.valueOf(superStep));
    for (Map.Entry<WritableComparable<?>, MsgBuffer> entry : msgBuffer
        .entrySet()) {
      File vertexDir = new File(superStepDir, entry.getKey().toString());
      entry.getValue().dump(new File(vertexDir, "inputs"));
    }
  }

  public Set<WritableComparable<?>> getVertexIDList() {
    return msgBuffer.keySet();
  }

  public boolean hasMessages() {
    return !msgBuffer.isEmpty();
  }

  class MsgIterable implements Iterable<Writable> {

    private WritableComparable<?> vertexId;

    public MsgIterable(WritableComparable<?> id) {
      this.vertexId = id;
    }

    @Override
    public Iterator<Writable> iterator() {
      // TODO Auto-generated method stub
      if (msgBuffer.containsKey(vertexId)) {
        return msgBuffer.get(vertexId).getMessages().iterator();
      } else {
        return new ArrayList<Writable>().iterator();
      }
    }

  }
}
