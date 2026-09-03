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

package com.aliyun.odps.storage.internal.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.data.Blob;
import com.aliyun.odps.storage.exceptions.BlobDownloadException;
import com.aliyun.odps.storage.internal.utils.IOUtils;
import com.aliyun.odps.storage.read.BlobDataIterator;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

/**
 *
 | BlobIteratorImpl.next() 返回一个 LimitedInputStream,
 |     它包装了下面的"干净的协议帧流"，限定了单个Blob的Data范围
 +-------------------------------------------------------------------------+
 | BlobIteratorImpl  (迭代器实现)                                          |
 |                                                                         |
 |   - 它消费的是一个“干净的协议帧流”。                                      |
 |   - 它的工作是解析 [HeaderLen][Header][DataLen][Data]... 结构。         |
 |   - 它不知道下面的 CRC 和 LZ4 的存在。                                    |
 +-------------------------------------------------------------------------+
 +-------------------------------------------------------------------------+
 | Lz4DecompressingInputStream (装饰器)                                    |
 |   - 它消费的是一个“去掉了CRC的LZ4压缩流”。                              |
 |   - 它输出的是“干净的协议帧流”。                                        |
 +-------------------------------------------------------------------------+
 +-------------------------------------------------------------------------+
 | CrcStrippedInputStream (装饰器)                                         |
 |   - 它消费的是最原始的“网络流”。                                        |
 |   - 它输出的是一个“去掉了CRC的LZ4压缩流”。                              |
 +-------------------------------------------------------------------------+
 +-------------------------------------------------------------------------+
 | 原始网络 InputStream (源头)                                             |
 |   - 包含了所有 Blob 的、经过CRC分块和LZ4压缩的单一数据流。                |
 +-------------------------------------------------------------------------+
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class BlobDataIteratorImpl implements BlobDataIterator {

  private static final Logger log = LoggerFactory.getLogger(BlobDataIteratorImpl.class);
  private static final Gson GSON = new Gson();

  private final DataInputStream sourceStream;
  private final List<Blob> orderedBlobRefs;
  private int blobIndex = 0;
  private boolean isFinished = false;

  private LimitedInputStream previousStream = null;

  public BlobDataIteratorImpl(InputStream sourceStream, List<Blob> orderedBlobRefs) {
    this.sourceStream = new DataInputStream(sourceStream);
    this.orderedBlobRefs = orderedBlobRefs;
  }

  @Override
  public boolean hasNext() {
    if (isFinished) {
      return false;
    }
    // 确保上一个 blob 的流和 footer 都被处理完毕
    ensurePreviousBlobIsConsumed();
    if (blobIndex < orderedBlobRefs.size()) {
      return true;
    } else {
      finish();
      return false;
    }
  }

  @Override
  public BlobDataStream next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more blobs to iterate.");
    }

    try {
      long headerLen = IOUtils.readLittleEndianLong(sourceStream);
      byte[] headerBytes = IOUtils.readNBytes(sourceStream, (int) headerLen);

      BlobHeader header = parseHeader(headerBytes);
      String mimeType = normalizeHeaderValue((header != null) ? header.mimeType : null);
      String customFileName = normalizeHeaderValue((header != null) ? header.customFileName : null);
      long dataLen = IOUtils.readLittleEndianLong(sourceStream);

      BlobDataStream currentStream = new BlobDataStream(sourceStream, dataLen, mimeType,
                                                         customFileName);
      this.previousStream = currentStream;
      this.blobIndex++;

      return currentStream;

    } catch (IOException e) {
      throw finishWithException(e);
    }
  }

  private static class BlobHeader {
    @SerializedName("ContentType")
    String mimeType;

    @SerializedName("CustomFileName")
    String customFileName;
  }

  private BlobHeader parseHeader(byte[] headerBytes) {
    if (headerBytes == null || headerBytes.length == 0) {
      return null;
    }
    try {
      BlobHeader header =
          GSON.fromJson(new String(headerBytes, StandardCharsets.UTF_8), BlobHeader.class);
      return header;
    } catch (Exception e) {
      log.warn("Failed to parse blob header: {}", e.getMessage());
      return null;
    }
  }

  private String normalizeHeaderValue(String value) {
    return (value == null || value.isEmpty()) ? null : value;
  }

  private void ensurePreviousBlobIsConsumed() {
    if (previousStream != null && !previousStream.isClosed()) {
      log.warn("InputStream for blob {} was not closed by the user. Forcing consumption.",
               orderedBlobRefs.get(blobIndex - 1).getReference().substring(0, 8));
      try {
        previousStream.close();
      } catch (IOException e) {
        throw finishWithException(e);
      }
    }

    if (previousStream != null) {
      try {
        long footerLen = IOUtils.readLittleEndianLong(sourceStream);
        byte[] footer = IOUtils.readNBytes(sourceStream, (int)footerLen);
        // deal with footer util footer is not empty
      } catch (IOException e) {
        throw finishWithException(e);
      } finally {
        previousStream = null;
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!isFinished) {
      isFinished = true;
      sourceStream.close();
    }
  }

  private void finish() {
    try {
      close();
    } catch (IOException e) {
      // 在内部静默关闭时忽略异常
    }
  }

  private BlobDownloadException finishWithException(Exception cause) {
    finish();
    Blob failedBlob = (blobIndex < orderedBlobRefs.size()) ? orderedBlobRefs.get(blobIndex) : null;
    return new BlobDownloadException("Download failed or was interrupted", cause, failedBlob);
  }
}
