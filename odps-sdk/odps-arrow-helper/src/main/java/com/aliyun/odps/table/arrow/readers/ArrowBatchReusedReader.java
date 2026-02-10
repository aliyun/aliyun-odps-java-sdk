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
import com.aliyun.odps.table.utils.ArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ArrowBatchReusedReader implements ArrowReader {

    private final org.apache.arrow.vector.ipc.ArrowReader arrowReader;
    private final List<FieldVector> decodeFieldVectors;

    private VectorSchemaRoot currentBatch;
    private Schema currentSchema;

    public ArrowBatchReusedReader(InputStream is,
                                  BufferAllocator allocator) {
        this(is, allocator, null);
    }

    public ArrowBatchReusedReader(InputStream is,
                                  BufferAllocator allocator,
                                  CompressionCodec.Factory compressionFactory) {
        this(is, allocator, compressionFactory, false, null);
    }

    public ArrowBatchReusedReader(InputStream is,
                                  BufferAllocator allocator,
                                  CompressionCodec.Factory compressionFactory,
                                  boolean async,
                                  BlockingQueue<Object> asyncQueue) {
        if (compressionFactory == null) {
            compressionFactory = NoCompressionCodec.Factory.INSTANCE;
        }
        this.arrowReader = async ? new ArrowStreamAsyncReader(is, allocator, compressionFactory, asyncQueue)
                : new ArrowStreamReader(is, allocator, compressionFactory);
        this.currentBatch = null;
        this.decodeFieldVectors = new ArrayList<>();
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
            if (!arrowReader.getDictionaryVectors().isEmpty()) {
                loadDictionaries();
            }
        } else {
            currentBatch = null;
        }
        if (currentSchema == null) {
            currentSchema = arrowReader.getVectorSchemaRoot().getSchema();
        }
        return hasNext;
    }

    @Override
    public void close() throws IOException {
        closeDecodeVectors();
        arrowReader.close();
    }

    @Override
    public long bytesRead() {
        return arrowReader.bytesRead();
    }

    @Override
    public void close(boolean closeReadSource) throws IOException {
        closeDecodeVectors();
        arrowReader.close(closeReadSource);
    }

    @Override
    public Schema getSchema() {
        return currentSchema;
    }

    private void closeDecodeVectors() {
        decodeFieldVectors.forEach(ValueVector::close);
        decodeFieldVectors.clear();
    }

    private void loadDictionaries() throws IOException {
        closeDecodeVectors();

        List<FieldVector> updateFieldVectors = new ArrayList<>();
        Map<Long, Dictionary> dictionaries = arrowReader.getDictionaryVectors();

        for (FieldVector vector : currentBatch.getFieldVectors()) {
            Field field = vector.getField();
            DictionaryEncoding encoding = field.getDictionary();
            List<FieldVector> children = vector.getChildrenFromFields();

            if (encoding == null) {
                if (!children.isEmpty()) {
                    for (FieldVector child : children) {
                        if (ArrowUtils.hasDictionaryEncoding(child)) {
                            throw new IOException("The dictionary encoding was not available for field: " + field.getName());
                        }
                    }
                }
                updateFieldVectors.add(vector);
            } else {
                if (!dictionaries.containsKey(encoding.getId())) {
                    throw new IOException("The dictionary was not available, id was: " + encoding.getId());
                }
                if (!children.isEmpty()) {
                    throw new IOException("The dictionary encoding was not available for field: " + field.getName());
                }

                Dictionary dict = dictionaries.get(encoding.getId());
                ValueVector newVector = ArrowUtils.decode(vector, dict);

                updateFieldVectors.add((FieldVector) newVector);
                decodeFieldVectors.add((FieldVector) newVector);
            }
        }

        this.currentBatch = new VectorSchemaRoot(updateFieldVectors);
    }
}
