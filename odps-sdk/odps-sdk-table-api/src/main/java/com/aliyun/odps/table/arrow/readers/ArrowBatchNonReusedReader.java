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
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.*;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.*;

public class ArrowBatchNonReusedReader implements ArrowReader {

    private final BufferAllocator allocator;
    private final MessageChannelReader messageReader;
    private final CompressionCodec.Factory compressionFactory;

    private boolean initialized = false;
    private int loadedDictionaryCount;
    private Map<Long, Dictionary> dictionaries;
    private VectorSchemaRoot currentBatch;
    private Schema originalSchema;
    private List<Field> fieldList;

    public ArrowBatchNonReusedReader(InputStream is,
                                     BufferAllocator allocator) {
        this(is, allocator, NoCompressionCodec.Factory.INSTANCE);
    }

    public ArrowBatchNonReusedReader(InputStream is,
                                     BufferAllocator allocator,
                                     CompressionCodec.Factory compressionFactory) {
        this.allocator = allocator;
        this.compressionFactory = compressionFactory;
        this.messageReader = new MessageChannelReader(new ReadChannel(Channels.newChannel(is)),
                this.allocator);
        this.currentBatch = null;
    }

    @Override
    public VectorSchemaRoot getCurrentValue() {
        return currentBatch;
    }

    @Override
    public boolean nextBatch() throws IOException {
        boolean hasNext = loadNextBatch();
        if (!hasNext) {
            this.currentBatch = null;
        }
        return hasNext;
    }

    @Override
    public void close() throws IOException {
        if (initialized) {
            for (Dictionary dictionary : dictionaries.values()) {
                dictionary.getVector().close();
            }
        }
        messageReader.close();
    }

    @Override
    public long bytesRead() {
        return messageReader.bytesRead();
    }


    /**
     * Load the next ArrowRecordBatch to the vector schema root if available.
     *
     * @return true if a batch was read, false on EOS
     * @throws IOException on error
     */
    private boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();
        MessageResult result = messageReader.readNext();

        // Reached EOS
        if (result == null) {
            return false;
        }

        if (result.getMessage().headerType() == MessageHeader.RecordBatch) {
            ArrowBuf bodyBuffer = result.getBodyBuffer();

            // For zero-length batches, need an empty buffer to deserialize the batch
            if (bodyBuffer == null) {
                bodyBuffer = allocator.getEmpty();
            }

            VectorLoader loader = new VectorLoader(currentBatch, compressionFactory);
            ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
            try {
                loader.load(batch);
            } finally {
                batch.close();
            }
            checkDictionaries();
            return true;
        } else if (result.getMessage().headerType() == MessageHeader.DictionaryBatch) {
            // if it's dictionary message, read dictionary message out and continue to read unless get a batch or eos.
            ArrowDictionaryBatch dictionaryBatch = readDictionary(result);
            loadDictionary(dictionaryBatch);
            loadedDictionaryCount++;
            return loadNextBatch();
        } else {
            throw new IOException("Expected RecordBatch or DictionaryBatch but header was " +
                    result.getMessage().headerType());
        }
    }


    /**
     * Ensure the reader has been initialized and reset the VectorSchemaRoot row count to 0.
     *
     * @throws IOException on error
     */
    private void prepareLoadNextBatch() throws IOException {
        if (!initialized) {
            initialize();
            this.initialized = true;
        }
        List<FieldVector> vectors = new ArrayList<>(fieldList.size());
        for (Field field : fieldList) {
            vectors.add(field.createVector(allocator));
        }
        Schema schema = new Schema(fieldList, originalSchema.getCustomMetadata());
        this.currentBatch = new VectorSchemaRoot(schema, vectors, 0);
        currentBatch.setRowCount(0);
    }

    /**
     * Reads the schema and initializes the fieldList.
     */
    private void initialize() throws IOException {
        this.originalSchema = readSchema();
        this.fieldList = new ArrayList<>(originalSchema.getFields().size());
        Map<Long, Dictionary> dictionaries = new HashMap<>();

        // Convert fields with dictionaries to have the index type
        for (Field field : originalSchema.getFields()) {
            Field updated = DictionaryUtility.toMemoryFormat(field, allocator, dictionaries);
            this.fieldList.add(updated);
        }
        this.dictionaries = Collections.unmodifiableMap(dictionaries);
    }

    private Schema readSchema() throws IOException {
        MessageResult result = messageReader.readNext();

        if (result == null) {
            throw new IOException("Unexpected end of input. Missing schema.");
        }

        if (result.getMessage().headerType() != MessageHeader.Schema) {
            throw new IOException("Expected schema but header was " + result.getMessage().headerType());
        }

        final Schema schema = MessageSerializer.deserializeSchema(result.getMessage());
        MetadataV4UnionChecker.checkRead(schema, MetadataVersion.fromFlatbufID(result.getMessage().version()));
        return schema;
    }

    private ArrowDictionaryBatch readDictionary(MessageResult result) throws IOException {

        ArrowBuf bodyBuffer = result.getBodyBuffer();

        // For zero-length batches, need an empty buffer to deserialize the batch
        if (bodyBuffer == null) {
            bodyBuffer = allocator.getEmpty();
        }

        return MessageSerializer.deserializeDictionaryBatch(result.getMessage(), bodyBuffer);
    }

    private void loadDictionary(ArrowDictionaryBatch dictionaryBatch) {
        long id = dictionaryBatch.getDictionaryId();
        Dictionary dictionary = dictionaries.get(id);
        if (dictionary == null) {
            throw new IllegalArgumentException("Dictionary ID " + id + " not defined in schema");
        }
        FieldVector vector = dictionary.getVector();
        // if is deltaVector, concat it with non-delta vector with the same ID.
        if (dictionaryBatch.isDelta()) {
            try (FieldVector deltaVector = vector.getField().createVector(allocator)) {
                load(dictionaryBatch, deltaVector);
                VectorBatchAppender.batchAppend(vector, deltaVector);
            }
            return;
        }

        load(dictionaryBatch, vector);
    }

    private void load(ArrowDictionaryBatch dictionaryBatch, FieldVector vector) {
        VectorSchemaRoot root = new VectorSchemaRoot(
                Collections.singletonList(vector.getField()),
                Collections.singletonList(vector), 0);
        VectorLoader loader = new VectorLoader(root);
        try {
            loader.load(dictionaryBatch.getDictionary());
        } finally {
            dictionaryBatch.close();
        }
    }

    /**
     * When read a record batch, check whether its dictionaries are available.
     */
    private void checkDictionaries() throws IOException {
        // if all dictionaries are loaded, return.
        if (loadedDictionaryCount == dictionaries.size()) {
            return;
        }
        for (FieldVector vector : this.currentBatch.getFieldVectors()) {
            DictionaryEncoding encoding = vector.getField().getDictionary();
            if (encoding != null) {
                // if the dictionaries it needs is not available and the vector is not all null, something was wrong.
                if (!dictionaries.containsKey(encoding.getId()) && vector.getNullCount() < vector.getValueCount()) {
                    throw new IOException("The dictionary was not available, id was:" + encoding.getId());
                }
            }
        }
    }
}
