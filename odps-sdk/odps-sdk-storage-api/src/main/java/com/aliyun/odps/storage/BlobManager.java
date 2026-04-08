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

package com.aliyun.odps.storage;

import java.io.InputStream;
import java.util.List;

import com.aliyun.odps.data.Blob;
import com.aliyun.odps.storage.exceptions.BlobDownloadException;
import com.aliyun.odps.storage.read.BlobDataIterator;

/**
 * Manager for blob download operations in MaxCompute Storage API.
 *
 * <p>This interface provides methods for downloading blobs from MaxCompute storage.
 * Both single blob download and batch download operations are supported.
 *
 * <p>Example usage for single blob download:
 * <pre>{@code
 * Blob blobRef = Blob.fromReference("blob_reference");
 * try (InputStream stream = blobManager.download(blobRef)) {
 *     // Read data from stream
 * }
 * }</pre>
 *
 * <p>Example usage for batch blob download:
 * <pre>{@code
 * List<Blob> blobs = Arrays.asList(blob1, blob2, blob3);
 * try (BlobDataIterator iterator = blobManager.batchDownload(blobs)) {
 *     while (iterator.hasNext()) {
 *         try (InputStream stream = iterator.next()) {
 *             // Read data from stream
 *         }
 *     }
 * }
 * }</pre>
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public interface BlobManager {

  /**
   * Downloads a single blob from MaxCompute storage.
   *
   * <p>The returned InputStream must be closed by the caller to release resources.
   * It is recommended to use try-with-resources to ensure the stream is properly closed.
   *
   * @param blobRef The blob reference to download
   * @return An InputStream containing the blob data
   * @throws BlobDownloadException if the blob download fails
   * @throws MaxStorageException if the blob does not exist or access is denied
   * @throws ClientException if there is a client-side error
   */
  InputStream download(Blob blobRef);

  /**
   * Downloads multiple blobs in batch.
   *
   * <p>The returned BlobDataIterator must be closed by the caller to release resources.
   * It is recommended to use try-with-resources to ensure the iterator is properly closed.
   *
   * <p>The order of blobs returned by the iterator matches the order of the input list.
   * If any blob fails to download, the iterator will throw a BlobDownloadException.
   *
   * @param blobs The list of blob references to download
   * @return A BlobDataIterator that provides InputStreams for each blob
   * @throws BlobDownloadException if any blob download fails
   * @throws MaxStorageException if any blob does not exist or access is denied
   * @throws ClientException if there is a client-side error
   */
  BlobDataIterator batchDownload(List<Blob> blobs);

}
