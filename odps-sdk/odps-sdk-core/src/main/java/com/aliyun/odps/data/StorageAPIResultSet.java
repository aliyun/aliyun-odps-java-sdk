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

package com.aliyun.odps.data;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.GeneralConfiguration;
import com.aliyun.odps.credentials.StaticCredentialProvider;
import com.aliyun.odps.storage.MaxStorageClient;
import com.aliyun.odps.storage.read.InstanceReadSession;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.aliyun.odps.table.InstanceIdentifier;

/**
 * A ResultSet implementation that reads data from an ODPS instance result
 * using the new Storage API (MaxStorageClient).
 * This ResultSet supports a client-side record limit and ensures resources are
 * automatically closed upon completion of iteration or when the limit is reached.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class StorageAPIResultSet extends ResultSet {

  private final Function<Object, Object> closeable;
  private long readCount = 0;
  // volatile 保证多线程可见性，确保 closed 状态被正确同步
  private volatile boolean closed = false;

  /**
   * @param recordIterator The iterator over records.
   * @param schema         The table schema.
   * @param recordCount    The total number of records to be read (acts as a LIMIT).
   *                       If set to -1, it reads until the end of the stream.
   * @param closeable      A function to be called to release resources.
   */
  @SuppressWarnings("unchecked")
  public StorageAPIResultSet(Iterator<Record> recordIterator,
                             TableSchema schema,
                             long recordCount,
                             Function<?, ?> closeable) {
    super(recordIterator, schema, recordCount);
    this.closeable = (Function<Object, Object>) closeable;
  }

  @Override
  public Record next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more records available, or limit has been reached.");
    }
    Record record = super.next();
    readCount++;
    return record;
  }

  /**
   * Checks if there are more records to be read, respecting the recordCount limit.
   * This method is self-closing: it will trigger resource cleanup when the iteration
   * ends either by reaching the limit or by exhausting the underlying data source.
   *
   * @return true if there are more records, false otherwise.
   */
  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }

    // 检查是否达到 recordCount 限制。此检查仅在 recordCount 为非负数时生效。
    boolean limitReached = (recordCount >= 0 && readCount >= recordCount);

    if (limitReached) {
      // 已达到限制，关闭资源并返回 false
      autoClose();
      return false;
    }

    boolean underlyingHasNext = super.hasNext();
    if (!underlyingHasNext) {
      // 底层数据已耗尽，关闭资源
      autoClose();
    }

    return underlyingHasNext;
  }

  /**
   * Helper method to call close() and wrap any checked exception into a RuntimeException.
   * This is useful for calling from hasNext() which cannot throw checked exceptions.
   */
  private void autoClose() {
    try {
      close();
    } catch (Exception e) {
      // hasNext() 接口不允许抛出受检异常，因此包装为非受检异常
      throw new RuntimeException("Failed to auto-close Storage API resources.", e);
    }
  }

  /**
   * Closes the underlying resources, including the record reader and the storage client.
   * This method is idempotent, meaning calling it multiple times has no effect after the first call.
   *
   * @throws Exception if an error occurs during the first closing attempt.
   */
  @Override
  public void close() throws Exception {
    if (closed) {
      return;
    }
    // 使用 try-finally 确保即使关闭操作失败，closed 标志也被设置，防止重复尝试
    try {
      if (closeable != null) {
        // The RuntimeException thrown from the lambda will propagate here.
        closeable.apply(null);
      }
      // 如果父类有 close 逻辑，也应该调用
      super.close();
    } finally {
      closed = true;
    }
  }


  public static StorageAPIResultSet of(Odps odps, GeneralConfiguration config, InstanceIdentifier instanceId,
                                       boolean enableLimit, Long recordLimit) {
    MaxStorageClient client = convertToMaxStorageClient(odps, config);
    InstanceReadSession session = client.createInstanceReadSessionBuilder(instanceId)
      .withEnableLimit(enableLimit)
      .build();

    RecordReader reader = session.createReaderBuilder()
      .withEnableLimit(enableLimit)
      .build()
      .getAsRecordReader();

    Function<Object, Object> closeAction = ignored -> {
      try {
        reader.close();
        client.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close Storage API resources.", e);
      }
      return null;
    };

    long recordCount = session.getRecordCount();
    if (recordLimit != null && recordLimit > 0 && recordLimit < recordCount) {
      recordCount = recordLimit;
    }

    return new StorageAPIResultSet(reader.iterator(), session.getTableSchema(),
                                   recordCount, closeAction);
  }

  public static StorageAPIResultSet of(Odps odps, GeneralConfiguration config, InstanceIdentifier instanceId,
                                       boolean enableLimit, String taskName, int queryId, Long recordLimit) {
    MaxStorageClient client = convertToMaxStorageClient(odps, config);
    InstanceReadSession session = client.createInstanceReadSessionBuilder(instanceId)
      .withEnableLimit(enableLimit)
      .build();

    RecordReader reader = session.createReaderBuilder()
      .withEnableLimit(enableLimit)
      .withTaskName(taskName)
      .withQueryId(queryId)
      .build()
      .getAsRecordReader();

    Function<Object, Object> closeAction = ignored -> {
      try {
        reader.close();
        client.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close Storage API resources.", e);
      }
      return null;
    };

    long recordCount = session.getRecordCount();
    if (recordLimit != null && recordLimit > 0 && recordLimit < recordCount) {
      recordCount = recordLimit;
    }

    return new StorageAPIResultSet(reader.iterator(), session.getTableSchema(),
                                   recordCount, closeAction);
  }

  private static MaxStorageClient convertToMaxStorageClient(Odps odps, GeneralConfiguration config) {
    return MaxStorageClient.builder()
      .endpoint(odps.getEndpoint())
      .tunnelEndpoint(odps.getTunnelEndpoint())
      .httpSettings(HttpSettings.newBuilder()
                      .withConnectTimeout(Duration.ofSeconds(config.getSocketConnectTimeout()))
                      .withReadTimeout(Duration.ofSeconds(config.getSocketTimeout()))
                      .build())
      .credentialsProvider(new StaticCredentialProvider(odps.getAccount().getCredentials()))
      .project(odps.getDefaultProject())
      .build();
  }
}
