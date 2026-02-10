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
 * Represents an error that occurs on the client side.
 * <p>
 * This exception is thrown when there are issues with the client-side operations,
 * such as network errors, invalid parameters, or other client-related problems.
 */
public class ClientException extends MaxStorageException {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new ClientException with the specified error message.
   *
   * @param errorMessage the error message
   */
  public ClientException(String errorMessage) {
    super(errorMessage);
  }

  /**
   * Constructs a new ClientException with the specified cause.
   *
   * @param exception the cause of this exception
   */
  public ClientException(Exception exception) {
    super(exception);
  }

  /**
   * Constructs a new ClientException with the specified error message and cause.
   *
   * @param errorMessage the error message
   * @param exception    the cause of this exception
   */
  public ClientException(String errorMessage, Throwable exception) {
    super(errorMessage, exception);
  }

  @Override
  public String toString() {
    return "ClientException {" +
           "errorMessage='" + getMessage() + '\'' +
           '}';
  }
}

