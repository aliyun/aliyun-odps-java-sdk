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

package com.aliyun.odps.storage.internal;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.data.Blob;
import com.aliyun.odps.storage.BlobManager;
import com.aliyun.odps.storage.internal.io.BlobDataIteratorImpl;
import com.aliyun.odps.storage.read.BlobDataIterator;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class BlobManagerImpl implements BlobManager {

  private StorageStub stub;


  public BlobManagerImpl(StorageStub stub) {
    this.stub = stub;
  }

  public InputStream download(Blob blobRef) {
    if (blobRef == null) {
          throw new IllegalArgumentException("blobRef cannot be null");
        }
        return stub.readBlobs(Collections.singletonList(blobRef.getReference()));
  }

  public BlobDataIterator batchDownload(List<Blob> blobs) {
    InputStream is =
      stub.readBlobs(blobs.stream().map(Blob::getReference).collect(Collectors.toList()));
    return new BlobDataIteratorImpl(is, blobs);
  }
}
