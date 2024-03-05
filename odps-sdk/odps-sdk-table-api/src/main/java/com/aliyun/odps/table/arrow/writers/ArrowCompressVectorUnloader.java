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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class that handles converting a {@link VectorSchemaRoot}
 * to a {@link ArrowRecordBatch}.
 */
class ArrowCompressVectorUnloader extends VectorUnloader {

    private final VectorSchemaRoot root;
    private final boolean includeNullCount;
    private final CompressionCodec codec;
    private final boolean alignBuffers;

    ArrowCompressVectorUnloader(
            VectorSchemaRoot root, boolean includeNullCount, CompressionCodec codec, boolean alignBuffers) {
        super(root, includeNullCount, codec, alignBuffers);
        this.root = root;
        this.includeNullCount = includeNullCount;
        this.codec = codec;
        this.alignBuffers = alignBuffers;
    }

    public ArrowRecordBatch getRecordBatch() {
        List<ArrowFieldNode> nodes = new ArrayList<>();
        List<ArrowBuf> buffers = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            appendNodes(vector, nodes, buffers);
        }
        return new ArrowCompressRecordBatch(
                root.getRowCount(), nodes, buffers, CompressionUtil.createBodyCompression(codec), alignBuffers);
    }

    private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
        nodes.add(new ArrowFieldNode(vector.getValueCount(), includeNullCount ? vector.getNullCount() : -1));
        List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
        int expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField().getType());
        if (fieldBuffers.size() != expectedBufferCount) {
            throw new IllegalArgumentException(String.format(
                    "wrong number of buffers for field %s in vector %s. found: %s",
                    vector.getField(), vector.getClass().getSimpleName(), fieldBuffers));
        }
        for (ArrowBuf buf : fieldBuffers) {
            // If the codec is CompressionCodec, it will return a new buffer, and also close the input buffer.
            // In that case, we need to retain the input buffer still to avoid modifying the source VectorSchemaRoot.
            buf.getReferenceManager().retain();
            buffers.add(codec.compress(vector.getAllocator(), buf));
        }
        for (FieldVector child : vector.getChildrenFromFields()) {
            appendNodes(child, nodes, buffers);
        }
    }
}
