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

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ArrowTunnelBufferedReader implements ArrowRecordReader, AutoCloseable {

    private long start;
    private long count;
    private final long batchSize;
    private final List<Column> columnList;
    private final BufferAllocator allocator;
    private final CompressOption option;
    private final TableTunnel.DownloadSession tableSession;
    private final boolean disableModifiedCheck;
    private final LinkedList<VectorSchemaRoot> rootBuffer = new LinkedList<>();
    private final LinkedList<Long> bytesReadBuffer = new LinkedList<>();
    // 用于计算每次读取到 buffer 时单次 read 的字节数，新开一个 reader 时清零
    private long curBytesRead = 0;
    // 用于记录已经读取的字节数，每次读取 buffer 时添加
    private long alreadyBytesRead = 0;

    /**
     * 构造此类对象
     *
     * @param columns              需要读取的列 {@link Column}
     * @param option               {@link CompressOption}
     * @param start                本次要读取记录的起始位置
     * @param count                本次要读取记录的数量
     * @param batchSize            每次读取的记录数量
     * @param session              本次读取所在 session
     * @param disableModifiedCheck 不检查下载的数据是否是表中最新数据
     * @throws IOException
     */
    public ArrowTunnelBufferedReader(long start, long count, long batchSize, List<Column> columns, TableTunnel.DownloadSession session,
                                     BufferAllocator allocator, CompressOption option, boolean disableModifiedCheck) {
        this.start = start;
        this.count = count;
        this.batchSize = batchSize <= 0 ? 1000 : batchSize;
        this.allocator = allocator;
        this.option = option;
        this.columnList = columns;
        this.tableSession = session;
        this.disableModifiedCheck = disableModifiedCheck;
    }

    @Override
    public VectorSchemaRoot read() throws IOException {
        try {
            if (rootBuffer.isEmpty()) {
                curBytesRead = 0;
                openReader();
                if (rootBuffer.isEmpty()) {
                    return null;
                }
            }
            assert !bytesReadBuffer.isEmpty();
            alreadyBytesRead += bytesReadBuffer.pollFirst();
            return rootBuffer.pollFirst();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long bytesRead() {
        return alreadyBytesRead;
    }

    private void openReader() throws IOException, TunnelException {
        if (count <= 0) {
            return;
        }
        long rootNum = Math.min(count, batchSize);
        ArrowRecordReader arrowReader;
        if (tableSession != null) {
            arrowReader =
                    tableSession.openArrowRecordReader(start, rootNum, columnList, allocator, option, disableModifiedCheck);
        } else {
            throw new IllegalArgumentException("Cannot create record reader if session is null.");
        }
        VectorSchemaRoot root = arrowReader.read();
        while (root != null) {
            rootBuffer.add(root);
            // add bytes of a vectorSchemaRoot, instead of all
            bytesReadBuffer.add(arrowReader.bytesRead() - curBytesRead);
            curBytesRead = arrowReader.bytesRead();
            root = arrowReader.read();
        }
        arrowReader.close();
        start += rootNum;
        count -= rootNum;
    }

    @Override
    public void close() throws IOException {
        rootBuffer.clear();
    }
}
