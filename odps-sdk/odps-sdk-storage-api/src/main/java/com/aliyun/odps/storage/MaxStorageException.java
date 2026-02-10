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

/**
 * Exception thrown when an error occurs during MaxCompute Storage API operations.
 *
 * <p>This exception extends {@link RuntimeException} and is thrown when the Storage API
 * returns an error response. It contains information about the request ID, error status,
 * and error details that can help diagnose issues with Storage API calls.
 */
public class MaxStorageException extends RuntimeException {

  public MaxStorageException() {
    super();
  }

  public MaxStorageException(Exception e) {
    super(e);
  }

  public MaxStorageException(String message) {
    super(message);
  }

  public MaxStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
