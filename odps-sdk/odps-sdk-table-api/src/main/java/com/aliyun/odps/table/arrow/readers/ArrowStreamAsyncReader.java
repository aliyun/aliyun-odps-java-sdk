/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.table.arrow.readers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;

/**
 * This class reads from an input stream and produces ArrowRecordBatches.
 * Modify version of {@link org.apache.arrow.vector.ipc.ArrowStreamReader}, add async thread to read data.
 */
public class ArrowStreamAsyncReader extends ArrowReader {

    private static final Object DONE_SENTINEL = new Object();

    private BlockingQueue<Object> asyncQueue;

    private ExecutorService asyncThread;

    private boolean isEnd;

    private MessageChannelReader messageReader;

    private int loadedDictionaryCount;

    /**
     * Constructs a streaming reader using a MessageChannelReader. Non-blocking.
     *
     * @param messageReader      reader used to get messages from a ReadChannel
     * @param allocator          to allocate new buffers
     * @param compressionFactory the factory to create compression codec.
     */
    public ArrowStreamAsyncReader(
            MessageChannelReader messageReader,
            BufferAllocator allocator,
            CompressionCodec.Factory compressionFactory) {
        this(messageReader, allocator, compressionFactory, null);
    }

    /**
     * Constructs a streaming reader using a MessageChannelReader. Non-blocking.
     *
     * @param messageReader      reader used to get messages from a ReadChannel
     * @param allocator          to allocate new buffers
     * @param compressionFactory the factory to create compression codec.
     * @param asyncQueue         the blocking queue used by async thread.
     */
    public ArrowStreamAsyncReader(
            MessageChannelReader messageReader,
            BufferAllocator allocator,
            CompressionCodec.Factory compressionFactory,
            BlockingQueue<Object> asyncQueue) {
        super(allocator, compressionFactory);
        this.messageReader = messageReader;
        this.asyncQueue = asyncQueue == null ? new LinkedBlockingDeque<>() : asyncQueue;
        this.asyncThread = Executors.newSingleThreadExecutor();
        startAsyncThread();
    }

    /**
     * Constructs a streaming reader using a MessageChannelReader. Non-blocking.
     *
     * @param messageReader reader used to get messages from a ReadChannel
     * @param allocator     to allocate new buffers
     */
    public ArrowStreamAsyncReader(MessageChannelReader messageReader, BufferAllocator allocator) {
        this(messageReader, allocator, NoCompressionCodec.Factory.INSTANCE);
    }

    /**
     * Constructs a streaming reader from a ReadableByteChannel input. Non-blocking.
     *
     * @param in                 ReadableByteChannel to read messages from
     * @param allocator          to allocate new buffers
     * @param compressionFactory the factory to create compression codec.
     */
    public ArrowStreamAsyncReader(
            ReadableByteChannel in,
            BufferAllocator allocator,
            CompressionCodec.Factory compressionFactory) {
        this(new MessageChannelReader(new ReadChannel(in), allocator), allocator,
                compressionFactory);
    }

    /**
     * Constructs a streaming reader from a ReadableByteChannel input. Non-blocking.
     *
     * @param in        ReadableByteChannel to read messages from
     * @param allocator to allocate new buffers
     */
    public ArrowStreamAsyncReader(ReadableByteChannel in, BufferAllocator allocator) {
        this(new MessageChannelReader(new ReadChannel(in), allocator), allocator);
    }

    /**
     * Constructs a streaming reader from a ReadableByteChannel input. Non-blocking.
     *
     * @param in                 InputStream to read messages from
     * @param allocator          to allocate new buffers
     * @param compressionFactory the factory to create compression codec.
     */
    public ArrowStreamAsyncReader(
            InputStream in, BufferAllocator allocator, CompressionCodec.Factory compressionFactory) {
        this(Channels.newChannel(in), allocator, compressionFactory);
    }

    /**
     * Constructs a streaming reader from a ReadableByteChannel input. Non-blocking.
     *
     * @param in                 InputStream to read messages from
     * @param allocator          to allocate new buffers
     * @param compressionFactory the factory to create compression codec.
     * @param asyncQueue         the blocking queue used by async thread.
     */
    public ArrowStreamAsyncReader(
            InputStream in, BufferAllocator allocator, CompressionCodec.Factory compressionFactory,
            BlockingQueue<Object> asyncQueue) {
        this(new MessageChannelReader(new ReadChannel(Channels.newChannel(in)), allocator),
                allocator, compressionFactory, asyncQueue);
    }

    /**
     * Constructs a streaming reader from an InputStream. Non-blocking.
     *
     * @param in        InputStream to read messages from
     * @param allocator to allocate new buffers
     */
    public ArrowStreamAsyncReader(InputStream in, BufferAllocator allocator) {
        this(Channels.newChannel(in), allocator);
    }

    /**
     * Get the number of bytes read from the stream since constructing the reader.
     *
     * @return number of bytes
     */
    @Override
    public long bytesRead() {
        return messageReader.bytesRead();
    }

    /**
     * Closes the underlying read source.
     *
     * @throws IOException on error
     */
    @Override
    protected void closeReadSource() throws IOException {
        messageReader.close();
        asyncThread.shutdownNow();
    }

    public void startAsyncThread() {
        isEnd = false;
        asyncThread.submit(() -> {
            try {
                ArrowRecordBatch nextBatch = getNextBatch();
                while (nextBatch != null) {
                    asyncQueue.put(nextBatch);
                    nextBatch = getNextBatch();
                }
                asyncQueue.put(DONE_SENTINEL);
            } catch (Exception e) {
                try {
                    asyncQueue.put(e);
                } catch (InterruptedException ignored2) {
                }
            }
        });
    }

    private ArrowRecordBatch getNextBatch() throws IOException {
        ensureInitialized();
        MessageResult result = messageReader.readNext();

        // Reached EOS
        if (result == null) {
            return null;
        }

        if (result.getMessage().headerType() == MessageHeader.RecordBatch) {
            ArrowBuf bodyBuffer = result.getBodyBuffer();

            // For zero-length batches, need an empty buffer to deserialize the batch
            if (bodyBuffer == null) {
                bodyBuffer = allocator.getEmpty();
            }

            return MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
        } else if (result.getMessage().headerType() == MessageHeader.DictionaryBatch) {
            // if it's dictionary message, read dictionary message out and continue to read unless get a
            // batch or eos.
            ArrowDictionaryBatch dictionaryBatch = readDictionary(result);
            loadDictionary(dictionaryBatch);
            loadedDictionaryCount++;
            return getNextBatch();
        } else {
            throw new IOException(
                    "Expected RecordBatch or DictionaryBatch but header was "
                            + result.getMessage().headerType());
        }
    }

    /**
     * Load the next ArrowRecordBatch to the vector schema root if available.
     *
     * @return true if a batch was read, false on EOS
     * @throws IOException on error
     */
    @Override
    public boolean loadNextBatch() throws IOException {
        if (isEnd) {
            return false;
        }
        Object o;
        try {
            o = asyncQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for next batch in async mode", e);
        }
        if (o.equals(DONE_SENTINEL)) {
            isEnd = true;
            return false;
        } else if (o instanceof IOException) {
            throw (IOException) o;
        } else if (o instanceof Throwable) {
            throw new RuntimeException((Throwable) o);
        } else {
            prepareLoadNextBatch(); // actually call root.setRowCount(0);
            ArrowRecordBatch batch = (ArrowRecordBatch) o;
            loadRecordBatch(batch);
            //checkDictionaries(); // FIXME: can be remove if confidence
            return true;
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
        for (FieldVector vector : getVectorSchemaRoot().getFieldVectors()) {
            DictionaryEncoding encoding = vector.getField().getDictionary();
            if (encoding != null) {
                // if the dictionaries it needs is not available and the vector is not all null, something
                // was wrong.
                if (!dictionaries.containsKey(encoding.getId())
                        && vector.getNullCount() < vector.getValueCount()) {
                    throw new IOException(
                            "The dictionary was not available, id was:" + encoding.getId());
                }
            }
        }
    }

    /**
     * Reads the schema message from the beginning of the stream.
     *
     * @return the deserialized arrow schema
     */
    @Override
    protected Schema readSchema() throws IOException {
        MessageResult result = messageReader.readNext();

        if (result == null) {
            throw new IOException("Unexpected end of input. Missing schema.");
        }

        if (result.getMessage().headerType() != MessageHeader.Schema) {
            throw new IOException(
                    "Expected schema but header was " + result.getMessage().headerType());
        }

        final Schema schema = MessageSerializer.deserializeSchema(result.getMessage());
        MetadataV4UnionChecker.checkRead(
                schema, MetadataVersion.fromFlatbufID(result.getMessage().version()));
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
}
