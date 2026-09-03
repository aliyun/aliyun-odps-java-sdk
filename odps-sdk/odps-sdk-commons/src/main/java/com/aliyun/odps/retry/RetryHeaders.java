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

package com.aliyun.odps.retry;

/** Header names used to correlate the requests in one SDK retry sequence. */
public final class RetryHeaders {

  /** Stable identifier shared by all requests in one retry sequence. */
  public static final String TRACE_ID = "odps-tunnel-retry-trace-id";

  /** Zero-based request index within one retry sequence. */
  public static final String RETRY_INDEX = "odps-tunnel-retry-index";

  private RetryHeaders() {
  }
}
