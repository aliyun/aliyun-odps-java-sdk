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
    // 非 null = 这一批的 reader 还没收尾成功；调用方 close() 时兜底关掉它。
    private ArrowRecordReader currentReader;
    // 本批收尾失败本身：不顶掉让本批失败的原始异常，只挂在它的 suppressed 上带出去。
    private IOException pendingCloseFailure;

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
        pendingCloseFailure = null;
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
            IOException wrapped = new IOException(e);
            attachCloseFailure(wrapped);
            throw wrapped;
        } catch (IOException | RuntimeException e) {
            attachCloseFailure(e);
            throw e;
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
        ArrowRecordReader arrowReader = openBatchReader(start, rootNum);
        currentReader = arrowReader;
        boolean batchSettled = false;
        try {
            VectorSchemaRoot root = arrowReader.read();
            while (root != null) {
                rootBuffer.add(root);
                // add bytes of a vectorSchemaRoot, instead of all
                bytesReadBuffer.add(arrowReader.bytesRead() - curBytesRead);
                curBytesRead = arrowReader.bytesRead();
                root = arrowReader.read();
            }
            arrowReader.close();
            batchSettled = true;
        } finally {
            // 批次读到一半抛错（Arrow IPC 流被截断、消息头反序列化失败都是这个形状）时也必须把这一批的
            // reader 关掉：ArrowTunnelRecordReader.close() 里的 inputStream.close() 与
            // connection.disconnect() 都挂在那里，原来没有 try/finally，一抛错就整段跳过。
            // 成功路径的关闭时机、bytesRead() 计数与游标推进保持原样。
            boolean settledOrRetried = true;
            if (!batchSettled) {
                try {
                    arrowReader.close();
                } catch (IOException | RuntimeException closeFailure) {
                    // 收尾失败不能顶掉让本批失败的原始异常：记下来挂到它身上带出去，
                    // currentReader 保持非 null，调用方 close() 时还会再试一次。
                    settledOrRetried = false;
                    rememberCloseFailure(closeFailure);
                }
            }
            if (settledOrRetried) {
                currentReader = null;
            }
        }
        start += rootNum;
        count -= rootNum;
    }

    /**
     * 开一个批次的 arrow reader。默认实现就是抽取前 openReader() 里的 session 分派，行为一字未改；
     * 留成包级方法是测试注入替身的接缝 —— {@link com.aliyun.odps.data.ArrowRecordReader} 的公开实现
     * {@link ArrowTunnelRecordReader} 构造必须发 HTTP 请求拿 session 与连接，测试里造不出一个
     * "读到第 N 个 root 抛错、并且能记下 close() 被调了几次"的替身，而漏掉的正是 close() 里的
     * inputStream.close() 与 connection.disconnect()。
     *
     * @see ArrowTunnelBufferedReader#openReader()
     */
    ArrowRecordReader openBatchReader(long readerStart, long readerCount) throws IOException, TunnelException {
        if (tableSession != null) {
            return tableSession.openArrowRecordReader(readerStart, readerCount, columnList, allocator,
                    option, disableModifiedCheck);
        }
        throw new IllegalArgumentException("Cannot create record reader if session is null.");
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        VectorSchemaRoot root;
        while ((root = rootBuffer.pollFirst()) != null) {
            try {
                root.close();
            } catch (RuntimeException e) {
                IOException closeFailure = new IOException("Failed to close buffered Arrow root", e);
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        bytesReadBuffer.clear();
        // 兜底：批次中途抛错且那次收尾没成功时，调用方的 close() 仍能把连接断掉；
        // 已经正常收尾过的批次（currentReader == null）在这里不会被二次 close。
        ArrowRecordReader unsettled = currentReader;
        if (unsettled != null) {
            currentReader = null;
            try {
                unsettled.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * 收尾失败本身先记下来：原始异常才是本轮要交给调用方的那一个。
     * IOException 原样保留，RuntimeException 形态（如底层 disconnect 抛出的运行时异常）包成 IOException。
     */
    private void rememberCloseFailure(Exception closeFailure) {
        pendingCloseFailure = closeFailure instanceof IOException
                ? (IOException) closeFailure
                : new IOException(closeFailure);
    }

    /**
     * 把记下的收尾失败挂到本轮正在传播的失败异常上：类型与 message 一字不变，调用方只多看到一条
     * suppressed，不至于对"连接到底有没有还回去"完全无信息。
     */
    private void attachCloseFailure(Throwable failure) {
        IOException pending = pendingCloseFailure;
        if (pending != null && pending != failure) {
            failure.addSuppressed(pending);
        }
        pendingCloseFailure = null;
    }
}
