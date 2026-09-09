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

package com.aliyun.odps.rest;

import java.io.IOException;

/** Raised when a REST request makes no write progress before its configured timeout. */
public class RestWriteTimeoutException extends IOException {

  private static final long serialVersionUID = 1L;

  private final int timeoutSeconds;
  private final long bytesProduced;

  public RestWriteTimeoutException(int timeoutSeconds,
                                   long bytesProduced,
                                   Throwable cause) {
    super(String.format(
        "REST request write timed out after %d seconds (bytesProduced=%d). "
            + "The server-side outcome is unknown; the request was not replayed.",
        timeoutSeconds,
        bytesProduced), cause);
    this.timeoutSeconds = timeoutSeconds;
    this.bytesProduced = bytesProduced;
  }

  public int getTimeoutSeconds() {
    return timeoutSeconds;
  }

  public long getBytesProduced() {
    return bytesProduced;
  }
}
