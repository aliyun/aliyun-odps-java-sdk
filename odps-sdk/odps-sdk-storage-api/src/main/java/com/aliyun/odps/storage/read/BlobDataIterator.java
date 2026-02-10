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

package com.aliyun.odps.storage.read;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * 一个用于按顺序迭代批量下载的 Blob 内容的迭代器。
 *
 * <p>此迭代器是 {@link AutoCloseable} 的，<b>必须</b>在 try-with-resources 语句中使用，
 * 以确保底层网络连接等资源在操作完成或发生错误时被正确关闭。
 *
 * <p>在迭代过程中，如果下载或解析失败，{@link #hasNext()} 或 {@link #next()}
 * 方法会抛出 {@link com.aliyun.odps.storage.exceptions.BlobDownloadException}。
 *
 * <p>示例用法:
 * <pre>{@code
 * try (BlobDataIterator iterator = blobManager.batchDownload(blobs)) {
 *     while (iterator.hasNext()) {
 *         try (InputStream blobContentStream = iterator.next()) {
 *             // 处理 blobContentStream...
 *         }
 *     }
 * } catch (BlobDownloadException e) {
 *     // 处理下载中断...
 * }
 * }</pre>
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public interface BlobDataIterator extends Iterator<InputStream>, AutoCloseable {

  @Override
  InputStream next();

  @Override
  void close() throws IOException;
}
