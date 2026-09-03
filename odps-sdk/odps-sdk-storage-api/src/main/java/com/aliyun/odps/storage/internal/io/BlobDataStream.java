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

import java.io.InputStream;

/**
 * An InputStream wrapper that carries MIME type metadata from the blob header.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class BlobDataStream extends LimitedInputStream {

  private final String mimeType;

  private final String customFileName;

  public BlobDataStream(InputStream source, long limit, String mimeType, String customFileName) {
    super(source, limit);
    this.mimeType = mimeType;
    this.customFileName = customFileName;
  }

  /**
   * Returns the MIME type of this blob data, as set during upload.
   *
   * @return the MIME type string (e.g. "image/png"), or {@code null} if not set
   */
  public String getMimeType() {
    return mimeType;
  }

  /**
   * Returns the custom file name of this blob data, as set during upload.
   *
   * @return the custom file name string, or {@code null} if not set
   */
  public String getCustomFileName() {
    return customFileName;
  }
}
