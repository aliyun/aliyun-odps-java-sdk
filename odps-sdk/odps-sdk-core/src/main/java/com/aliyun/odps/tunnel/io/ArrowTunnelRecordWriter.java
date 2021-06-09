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

package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.ArrowRecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

public class ArrowTunnelRecordWriter implements ArrowRecordWriter {

    private TableTunnel.UploadSession tableSession;
    private ArrowHttpOutputStream outputStream;
    private Connection connection;
    private boolean isClosed;
    private CompressOption compress;

    public ArrowTunnelRecordWriter(TableTunnel.UploadSession tableSession, Connection connection, CompressOption option) {
        this.tableSession = tableSession;
        this.connection = connection;
        this.isClosed = false;
        this.compress = option;
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        if (isClosed) {
            throw new IOException("Arrow writer is closed");
        }
        if (outputStream == null) {
            OutputStream wr = this.connection.getOutputStream();
            if (compress != null && !compress.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_RAW)) {
                if (compress.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
                    Deflater def = new Deflater();
                    def.setLevel(compress.level);
                    def.setStrategy(compress.strategy);
                    wr = new DeflaterOutputStream(wr, def);
                } else if (compress.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_SNAPPY)) {
                    wr = new SnappyFramedOutputStream(wr);
                } else {
                    throw new IOException("invalid compression option.");
                }
            }
            outputStream = new ArrowHttpOutputStream(wr);
        }
        if (root.getRowCount() == 0) {
            return;
        }
        WriteChannel writeChannel = new WriteChannel(outputStream);
        VectorUnloader loader = new VectorUnloader(root);
        ArrowRecordBatch recordBatch = loader.getRecordBatch();
        try {
            MessageSerializer.serialize(writeChannel, recordBatch);
        } catch (IOException e) {
            throw new IOException("ArrowHttpOutputStream Serialize Exception", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            try {
                if (outputStream != null) {
                    outputStream.close();
                }
                Response response = connection.getResponse();
                if (!response.isOK()) {
                    TunnelException exception = new TunnelException(response.getHeader(HEADER_ODPS_REQUEST_ID), connection.getInputStream(),
                            response.getStatus());
                    throw new IOException(exception.getMessage(), exception);
                }
            } finally {
                connection.disconnect();
                isClosed = true;
            }
        }
    }
}
