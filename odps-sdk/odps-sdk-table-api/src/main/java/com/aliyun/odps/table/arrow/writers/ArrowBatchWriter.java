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

import com.aliyun.odps.table.arrow.ArrowWriter;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

public class ArrowBatchWriter implements ArrowWriter {

    private final WriteChannel out;
    private final IpcOption option;
    private boolean started;
    private boolean ended;
    private final CompressionUtil.CodecType codecType;

    public ArrowBatchWriter(OutputStream out) {
        this(out, new IpcOption());
    }

    public ArrowBatchWriter(OutputStream out, IpcOption option) {
        this(out, option, CompressionUtil.CodecType.NO_COMPRESSION);
    }

    public ArrowBatchWriter(OutputStream out, CompressionUtil.CodecType codecType) {
        this(out, new IpcOption(), codecType);
    }

    public ArrowBatchWriter(OutputStream out, IpcOption option, CompressionUtil.CodecType codecType) {
        this.out = new WriteChannel(Channels.newChannel(out));
        this.option = option;
        this.started = false;
        this.ended = false;
        this.codecType = codecType;
    }

    @Override
    public void writeBatch(VectorSchemaRoot root) throws IOException {
        VectorUnloader unloader;
        if (codecType.equals(CompressionUtil.CodecType.NO_COMPRESSION)) {
            unloader = new VectorUnloader(root);
        } else {
            // TODO: arrow 12.0 support compress unloader, remove it
            // See: https://github.com/apache/arrow/pull/15223
            unloader = new ArrowCompressVectorUnloader(root, true,
                    CommonsCompressionFactory.INSTANCE.createCodec(codecType), true);
        }
        ensureStarted(root);
        // TODO: validate root schema
        try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
            MessageSerializer.serialize(out, batch, option);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (started) {
                ensureEnded();
            }
            out.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public long bytesWritten() {
        return out.getCurrentPosition();
    }

    private void ensureStarted(VectorSchemaRoot root) throws IOException {
        if (!started) {
            started = true;
            MetadataV4UnionChecker.checkForUnion(root.getSchema().getFields().iterator(), option.metadataVersion);
            MessageSerializer.serialize(out, root.getSchema(), option);
        }
    }

    private void ensureEnded() throws IOException {
        if (!ended) {
            ended = true;
            if (!option.write_legacy_ipc_format) {
                out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
            }
            out.writeIntLittleEndian(0);
        }
    }
}
