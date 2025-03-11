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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamWriter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordPack;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TunnelMetrics;

/**
 * 用 Protobuf 序列化存储的 {@link RecordPack}
 * 和 TableTunnel 共同使用
 * 比直接使用 List<Record> 更加节省内存
 */
public class ProtobufRecordPack extends RecordPack {

  private ProtobufRecordStreamWriter writer;
  private ByteArrayOutputStream byteos;
  private long count = 0;
  private TableSchema schema;
  private CompressOption option = null;
  private boolean isComplete = false;
  private boolean shouldTransform = false;
  private TunnelMetrics accumulatedMetrics;
  private long localWallTimeMs;
  private long netWorkWallTimeMs;

  public void checkTransConsistency(boolean expect) throws IOException {
    if (shouldTransform != expect) {
      throw new IOException("RecordPack breaks the restriction of session. Try session.newRecordPack()");
    }
  }
  /**
   * 新建一个ProtobufRecordPack
   *
   * @param schema
   * @throws IOException
   */
  public ProtobufRecordPack(TableSchema schema) throws IOException {
    this(schema, new Checksum());
  }

  /**
   * 新建一个 ProtobufRecordPack，用对应的 CheckSum 初始化
   *
   * @param schema
   * @param checkSum
   * @throws IOException
   */
  public ProtobufRecordPack(TableSchema schema, Checksum checkSum) throws IOException {
    this(schema, checkSum, 0);
  }

  /**
   * 新建一个 ProtobufRecordPack，用对应的 CheckSum 初始化, 并且预设流 buffer 大小为 capacity
   *
   * @param schema
   * @param checkSum
   * @param capacity
   * @throws IOException
   */
  public ProtobufRecordPack(TableSchema schema, Checksum checkSum, int capacity)
      throws IOException {
    this(schema, checkSum, capacity, null);
  }

  /**
   * 新建一个 ProtobufRecordPack，用对应的 CheckSum 初始化, 数据压缩方式 option
   *
   * @param schema
   * @param checksum
   * @param option
   * @throws IOException
   */
  public ProtobufRecordPack(TableSchema schema, Checksum checksum, CompressOption option)
    throws IOException {
    this(schema, checksum, 0, option);
  }

  /**
   * 新建一个 ProtobufRecordPack，用对应的 CheckSum 初始化, 数据压缩方式 option, 并且预设流 buffer 大小为 capacity
   *
   * @param schema
   * @param checksum
   * @param capacity
   * @param option
   * @throws IOException
   */
  public ProtobufRecordPack(TableSchema schema, Checksum checksum, int capacity, CompressOption option)
    throws IOException {
    isComplete = false;
    if (capacity == 0) {
      byteos = new ByteArrayOutputStream();
    } else {
      byteos = new ByteArrayOutputStream(capacity);
    }

    this.schema = schema;
    if (null != option) {
      this.option = option;
    } else {
      this.option = new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
    }

    writer = new ProtobufRecordStreamWriter(schema, byteos, this.option);
    if (null != checksum) {
      writer.setCheckSum(checksum);
    }
    accumulatedMetrics = new TunnelMetrics();
  }

  public void setTransform(boolean shouldTransform) {
    this.shouldTransform = shouldTransform;
    this.writer.setTransform(shouldTransform);
  }

  @Override
  public void append(Record a) throws IOException {
    writer.write(a);
    ++count;
  }

  /**
   * 获取 RecordReader 对象
   * ProtobufRecordPack 不支持改方法
   *
   * @throws UnsupportedOperationException
   */
  @Override
  public RecordReader getRecordReader() throws IOException {
    throw new UnsupportedOperationException("PBPack does not supported Read.");
  }

  // FIXME: 返回的并不是 probuf 的 stream，而是 protobuf 输出的那个缓冲区
  public ByteArrayOutputStream getProtobufStream() throws IOException {
    if (!isComplete) {
      writer.flush();
    }
    return byteos;
  }

  public void complete() throws IOException {
    if (!isComplete) {
      writer.close();
      isComplete = true;
    }
  }

  public CompressOption getCompressOption() {
    return this.option;
  }

  /**
   * 获取当前 pack 在内存缓冲区中的大小
   *
   * 注意：由于在写到内存缓冲区前，数据会经过两个缓冲区（protobuf 和 defalter）
   * 因此这个值的变化并不是连续的
   *
   * @return
   */
  public long getTotalBytes() {
    return byteos.size();
  }

  /**
   * 获取输出数据序列化后的字节数
   *
   * @return
   * @throws IOException
   */
  protected long getTotalBytesWritten() throws IOException {
    writer.flush();
    return writer.getTotalBytes();
  }

  /**
   * 获取 Record 的 CheckSum
   */
  public Checksum getCheckSum() {
    return writer.getCheckSum();
  }

  /**
   * 清空 RecordPack
   */
  public void reset() throws IOException {
    if (byteos != null) {
      byteos.reset();
    }
    count = 0;
    this.writer = new ProtobufRecordStreamWriter(schema, byteos, option);
    this.writer.setTransform(shouldTransform);
    isComplete = false;
  }

  public boolean isComplete() {
    return isComplete;
  }

  /**
   * 清空 RecordPack
   *
   * @param checksum
   *     初始化 checksum
   */
  public void reset(Checksum checksum) throws IOException {
    reset();

    if (checksum != null) {
      this.writer.setCheckSum(checksum);
    }
  }

  /**
   * 返回 pack 内的 record 数量
   * @return
   */
  public long getSize() {
    return count;
  }

  public TunnelMetrics getMetrics() {
    return accumulatedMetrics;
  }

  public void addLocalWallTimeMs(long localWallTimeMs) {
    this.localWallTimeMs += localWallTimeMs;
  }

  public long getLocalWallTimeMs() {
    return localWallTimeMs;
  }

  public void addNetworkWallTimeMs(long netWorkWallTimeMs) {
    this.netWorkWallTimeMs += netWorkWallTimeMs;
  }

  public long getNetworkWallTimeMs() {
    return netWorkWallTimeMs;
  }

  public void addMetrics(TunnelMetrics batchMetrics) {
    accumulatedMetrics.add(batchMetrics);
    localWallTimeMs = 0;
    netWorkWallTimeMs = 0;
  }
}
