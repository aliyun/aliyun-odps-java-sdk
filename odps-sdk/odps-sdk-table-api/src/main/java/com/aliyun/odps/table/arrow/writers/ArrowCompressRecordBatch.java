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

package com.aliyun.odps.table.arrow.writers;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.util.List;

class ArrowCompressRecordBatch extends ArrowRecordBatch {

    private boolean closed;
    private final List<ArrowBuf> buffers;

    ArrowCompressRecordBatch(int length,
                             List<ArrowFieldNode> nodes,
                             List<ArrowBuf> buffers,
                             ArrowBodyCompression bodyCompression,
                             boolean alignBuffers) {
        super(length, nodes, buffers, bodyCompression, alignBuffers);
        this.buffers = buffers;
        this.closed = false;
    }

    @Override
    public void close() {
        super.close();
        if (!this.closed) {
            this.closed = true;
            // for compress buf
            for (ArrowBuf arrowBuf : buffers) {
                arrowBuf.getReferenceManager().release();
            }
        }
    }
}
