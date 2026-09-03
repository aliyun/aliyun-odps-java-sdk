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
import java.util.concurrent.ConcurrentLinkedQueue;

public class ArrowBatchNonReusedReader implements ArrowReader {

    private final BufferAllocator allocator;
    private final MessageChannelReader messageReader;
    private final CompressionCodec.Factory compressionFactory;

    private boolean initialized = false;
    private int loadedDictionaryCount;
    private Map<Long, Dictionary> dictionaries;
    private VectorSchemaRoot currentBatch;
    // Ownership transfers to the caller only when getCurrentValue() returns this batch.
    private boolean currentBatchClaimed;
    private Schema originalSchema;
    private List<Field> fieldList;
    private boolean hasDictionaries = false;
    private boolean dictionariesClosed;
    private boolean messageReaderClosed;
    private volatile Throwable terminalFailure;
    private final ConcurrentLinkedQueue<FieldVector> retainedFieldVectors =
            new ConcurrentLinkedQueue<>();

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
        throwTerminalFailureUnchecked();
        if (currentBatch != null) {
            currentBatchClaimed = true;
        }
        return currentBatch;
    }

    @Override
    public Schema getSchema() {
        return originalSchema;
    }

    @Override
    public boolean nextBatch() throws IOException {
        throwTerminalFailure();
        releaseCurrentBatchBeforeAdvance();
        try {
            return loadNextBatch();
        } catch (IOException | RuntimeException | Error e) {
            terminalFailure = e;
            closeUnpublishedBatch(e);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        close(true);
    }

    @Override
    public void close(boolean closeReadSource) throws IOException {
        Throwable failure = releaseUnclaimedCurrentBatch(null);
        failure = closeRetainedFieldVectors(failure);
        failure = closeDictionaries(failure);

        if (closeReadSource && !messageReaderClosed) {
            try {
                messageReader.close();
                messageReaderClosed = true;
            } catch (IOException | RuntimeException | Error closeError) {
                failure = addFailure(failure, closeError);
            }
        }

        throwIfCloseFailed(failure);
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
        if (!initialized) {
            initialize();
            this.initialized = true;
        }
        MessageResult result = messageReader.readNext();

        // Reached EOS
        if (result == null) {
            return false;
        }

        if (result.getMessage().headerType() == MessageHeader.RecordBatch) {
            ArrowRecordBatch batch = readRecordBatch(result);
            loadRecordBatch(batch);
            checkDictionaries();

            if (hasDictionaries) {
                loadDictionaries();
            }
            return true;
        } else if (result.getMessage().headerType() == MessageHeader.DictionaryBatch) {
            // if it's dictionary message, read dictionary message out and continue to read unless get a batch or eos.
            ArrowDictionaryBatch dictionaryBatch = readDictionary(result);
            loadDictionary(dictionaryBatch);
            loadedDictionaryCount++;
            return loadNextBatch();
        } else {
            IOException failure = new IOException(
                    "Expected RecordBatch or DictionaryBatch but header was "
                            + result.getMessage().headerType());
            closeBodyBuffer(result.getBodyBuffer(), failure);
            throw failure;
        }
    }


    /** Creates an empty root for a record batch that has not been exposed to the caller. */
    private VectorSchemaRoot createBatchRoot() {
        List<FieldVector> vectors = new ArrayList<>(fieldList.size());
        try {
            for (Field field : fieldList) {
                vectors.add(field.createVector(allocator));
            }
            Schema schema = new Schema(fieldList, originalSchema.getCustomMetadata());
            VectorSchemaRoot batch = new VectorSchemaRoot(schema, vectors, 0);
            batch.setRowCount(0);
            return batch;
        } catch (RuntimeException | Error e) {
            for (FieldVector vector : vectors) {
                closeFieldVectorOrRetain(vector, e);
            }
            throw e;
        }
    }

    private ArrowRecordBatch readRecordBatch(MessageResult result) throws IOException {
        ArrowBuf bodyBuffer = result.getBodyBuffer();

        // For zero-length batches, need an empty buffer to deserialize the batch
        if (bodyBuffer == null) {
            bodyBuffer = allocator.getEmpty();
        }

        try {
            return MessageSerializer.deserializeRecordBatch(
                    result.getMessage(), bodyBuffer);
        } catch (IOException | RuntimeException | Error e) {
            closeBodyBuffer(bodyBuffer, e);
            throw e;
        }
    }

    private void loadRecordBatch(ArrowRecordBatch batch) {
        Throwable failure = null;
        try {
            currentBatch = createBatchRoot();
            currentBatchClaimed = false;
            VectorLoader loader = new VectorLoader(currentBatch, compressionFactory);
            loader.load(batch);
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                batch.close();
            } catch (RuntimeException | Error closeError) {
                if (failure == null) {
                    throw closeError;
                }
                failure.addSuppressed(closeError);
            }
        }
    }

    private void releaseCurrentBatchBeforeAdvance() {
        VectorSchemaRoot batch = currentBatch;
        boolean claimed = currentBatchClaimed;
        if (batch == null) {
            return;
        }
        if (claimed) {
            currentBatch = null;
            currentBatchClaimed = false;
        } else {
            batch.close();
            currentBatch = null;
            currentBatchClaimed = false;
        }
    }

    private Throwable releaseUnclaimedCurrentBatch(Throwable failure) {
        VectorSchemaRoot batch = currentBatch;
        boolean claimed = currentBatchClaimed;
        if (batch == null) {
            return failure;
        }
        if (claimed) {
            currentBatch = null;
            currentBatchClaimed = false;
        } else {
            try {
                batch.close();
                currentBatch = null;
                currentBatchClaimed = false;
            } catch (RuntimeException | Error closeError) {
                failure = addFailure(failure, closeError);
            }
        }
        return failure;
    }

    private void closeUnpublishedBatch(Throwable failure) {
        VectorSchemaRoot batch = currentBatch;
        if (batch != null) {
            try {
                batch.close();
                currentBatch = null;
                currentBatchClaimed = false;
            } catch (RuntimeException | Error closeError) {
                failure.addSuppressed(closeError);
            }
        }
    }

    /**
     * Reads the schema and initializes the fieldList.
     */
    private void initialize() throws IOException {
        this.originalSchema = readSchema();
        List<Field> fields = new ArrayList<>(originalSchema.getFields().size());
        Map<Long, Dictionary> dictionaryMap = new HashMap<>();
        // Publish the live backing map before dictionary vectors are allocated. If
        // initialization or a first close attempt fails, close() must retain a path to retry
        // every vector instead of losing the only strong references with this stack frame.
        this.dictionaries = Collections.unmodifiableMap(dictionaryMap);

        try {
            // Convert fields with dictionaries to have the index type
            for (Field field : originalSchema.getFields()) {
                Field updated =
                        DictionaryUtility.toMemoryFormat(field, allocator, dictionaryMap);
                fields.add(updated);
            }
        } catch (RuntimeException | Error e) {
            closeDictionaries(e);
            throw e;
        }
        this.fieldList = fields;
        this.hasDictionaries = !dictionaryMap.isEmpty();
    }

    private Schema readSchema() throws IOException {
        MessageResult result = messageReader.readNext();

        if (result == null) {
            throw new IOException("Unexpected end of input. Missing schema.");
        }

        if (result.getMessage().headerType() != MessageHeader.Schema) {
            IOException failure = new IOException(
                    "Expected schema but header was " + result.getMessage().headerType());
            closeBodyBuffer(result.getBodyBuffer(), failure);
            throw failure;
        }

        final Schema schema;
        try {
            schema = MessageSerializer.deserializeSchema(result.getMessage());
            MetadataV4UnionChecker.checkRead(
                    schema, MetadataVersion.fromFlatbufID(result.getMessage().version()));
        } catch (IOException | RuntimeException | Error e) {
            closeBodyBuffer(result.getBodyBuffer(), e);
            throw e;
        }
        closeBodyBuffer(result.getBodyBuffer(), null);
        return schema;
    }

    private ArrowDictionaryBatch readDictionary(MessageResult result) throws IOException {
        ArrowBuf bodyBuffer = result.getBodyBuffer();

        // For zero-length batches, need an empty buffer to deserialize the batch
        if (bodyBuffer == null) {
            bodyBuffer = allocator.getEmpty();
        }

        try {
            return MessageSerializer.deserializeDictionaryBatch(
                    result.getMessage(), bodyBuffer);
        } catch (IOException | RuntimeException | Error e) {
            closeBodyBuffer(bodyBuffer, e);
            throw e;
        }
    }

    private void loadDictionary(ArrowDictionaryBatch dictionaryBatch) {
        Throwable failure = null;
        try {
            long id = dictionaryBatch.getDictionaryId();
            Dictionary dictionary = dictionaries.get(id);
            if (dictionary == null) {
                throw new IllegalArgumentException(
                        "Dictionary ID " + id + " not defined in schema");
            }
            FieldVector vector = dictionary.getVector();
            // if is deltaVector, concat it with non-delta vector with the same ID.
            if (dictionaryBatch.isDelta()) {
                FieldVector deltaVector = vector.getField().createVector(allocator);
                Throwable deltaFailure = null;
                try {
                    load(dictionaryBatch, deltaVector);
                    VectorBatchAppender.batchAppend(vector, deltaVector);
                } catch (RuntimeException | Error e) {
                    deltaFailure = e;
                    throw e;
                } finally {
                    closeFieldVectorOrRetain(deltaVector, deltaFailure);
                }
            } else {
                load(dictionaryBatch, vector);
            }
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                dictionaryBatch.close();
            } catch (RuntimeException | Error closeError) {
                if (failure == null) {
                    throw closeError;
                }
                failure.addSuppressed(closeError);
            }
        }
    }

    private void load(ArrowDictionaryBatch dictionaryBatch, FieldVector vector) {
        VectorSchemaRoot root = new VectorSchemaRoot(
                Collections.singletonList(vector.getField()),
                Collections.singletonList(vector), 0);
        VectorLoader loader = new VectorLoader(root, compressionFactory);
        loader.load(dictionaryBatch.getDictionary());
    }

    private Throwable closeDictionaries(Throwable failure) {
        if (dictionariesClosed || dictionaries == null) {
            return failure;
        }
        boolean closeFailed = false;
        for (Dictionary dictionary : dictionaries.values()) {
            try {
                dictionary.getVector().close();
            } catch (RuntimeException | Error closeError) {
                closeFailed = true;
                failure = addFailure(failure, closeError);
            }
        }
        dictionariesClosed = !closeFailed;
        return failure;
    }

    private Throwable closeRetainedFieldVectors(Throwable failure) {
        List<FieldVector> vectors = new ArrayList<>(retainedFieldVectors);
        for (FieldVector vector : vectors) {
            try {
                vector.close();
            } catch (RuntimeException | Error closeError) {
                failure = addFailure(failure, closeError);
                continue;
            }
            if (!retainedFieldVectors.remove(vector)) {
                failure = addFailure(
                        failure,
                        new IllegalStateException(
                                "Retained Arrow vector queue changed unexpectedly"));
            }
        }
        return failure;
    }

    private void closeFieldVectorOrRetain(
            FieldVector vector,
            Throwable failure) {
        try {
            vector.close();
        } catch (RuntimeException | Error closeError) {
            retainedFieldVectors.add(vector);
            if (failure == null) {
                throw closeError;
            }
            failure.addSuppressed(closeError);
        }
    }

    private void closeBodyBuffer(ArrowBuf bodyBuffer, Throwable failure) {
        if (bodyBuffer == null) {
            return;
        }
        try {
            bodyBuffer.close();
        } catch (RuntimeException | Error closeError) {
            if (failure == null) {
                throw closeError;
            }
            failure.addSuppressed(closeError);
        }
    }

    private Throwable addFailure(Throwable failure, Throwable closeError) {
        if (failure == null) {
            return closeError;
        }
        if (failure != closeError) {
            failure.addSuppressed(closeError);
        }
        return failure;
    }

    private void throwIfCloseFailed(Throwable failure) throws IOException {
        if (failure == null) {
            return;
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("Failed to close Arrow batch reader", failure);
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

    private void loadDictionaries() throws IOException {
        List<FieldVector> updateFieldVectors = new ArrayList<>();
        List<FieldVector> decodedVectors = new ArrayList<>();
        List<FieldVector> encodedVectors = new ArrayList<>();

        try {
            for (FieldVector vector : currentBatch.getFieldVectors()) {
                Field field = vector.getField();
                DictionaryEncoding encoding = field.getDictionary();
                List<FieldVector> children = vector.getChildrenFromFields();

                if (encoding == null) {
                    if (!children.isEmpty()) {
                        for (FieldVector child : children) {
                            if (ArrowUtils.hasDictionaryEncoding(child)) {
                                throw new IOException(
                                        "The dictionary encoding was not available for field: "
                                                + field.getName());
                            }
                        }
                    }
                    updateFieldVectors.add(vector);
                } else {
                    if (!dictionaries.containsKey(encoding.getId())) {
                        throw new IOException(
                                "The dictionary was not available, id was: "
                                        + encoding.getId());
                    }
                    if (!children.isEmpty()) {
                        throw new IOException(
                                "The dictionary encoding was not available for field: "
                                        + field.getName());
                    }

                    Dictionary dict = dictionaries.get(encoding.getId());
                    FieldVector decoded = (FieldVector) ArrowUtils.decode(vector, dict);

                    decodedVectors.add(decoded);
                    encodedVectors.add(vector);
                    updateFieldVectors.add(decoded);
                }
            }

            for (FieldVector vector : encodedVectors) {
                vector.close();
            }
            this.currentBatch = new VectorSchemaRoot(updateFieldVectors);
        } catch (IOException | RuntimeException | Error e) {
            for (FieldVector vector : decodedVectors) {
                closeFieldVectorOrRetain(vector, e);
            }
            throw e;
        }
    }

    private void throwTerminalFailure() throws IOException {
        Throwable failure = terminalFailure;
        if (failure == null) {
            return;
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("Arrow reader cannot continue after a prior failure", failure);
    }

    private void throwTerminalFailureUnchecked() {
        Throwable failure = terminalFailure;
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IllegalStateException(
                "Arrow reader cannot expose data after a prior failure", failure);
    }
}
