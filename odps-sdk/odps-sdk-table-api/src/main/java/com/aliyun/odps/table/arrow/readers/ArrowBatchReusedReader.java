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

package com.aliyun.odps.table.arrow.readers;

import com.aliyun.odps.table.arrow.ArrowReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.IOException;
import java.io.InputStream;
public class ArrowBatchReusedReader implements ArrowReader {

    private final ArrowStreamReader arrowReader;
    private VectorSchemaRoot currentBatch;

    public ArrowBatchReusedReader(InputStream is,
                                  BufferAllocator allocator) {
        this.arrowReader = new ArrowStreamReader(is, allocator);
        this.currentBatch = null;
    }

    public ArrowBatchReusedReader(InputStream is,
                                  BufferAllocator allocator,
                                  CompressionCodec.Factory compressionFactory) {
        this.arrowReader = new ArrowStreamReader(is, allocator, compressionFactory);
        this.currentBatch = null;
    }

    @Override
    public VectorSchemaRoot getCurrentValue() {
        return currentBatch;
    }

    @Override
    public boolean nextBatch() throws IOException {
        boolean hasNext = arrowReader.loadNextBatch();
        if (hasNext) {
            currentBatch = arrowReader.getVectorSchemaRoot();
        } else {
            currentBatch = null;
        }
        return hasNext;
    }

    @Override
    public void close() throws IOException {
        arrowReader.close();
    }

    @Override
    public long bytesRead() {
        return arrowReader.bytesRead();
    }
}
