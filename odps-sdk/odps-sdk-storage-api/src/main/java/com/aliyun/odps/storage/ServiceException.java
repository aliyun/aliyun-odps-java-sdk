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
 * Represents a specific, actionable error returned from the server.
 * <p>
 * It extends OdpsException and adds context information such as HTTP status code,
 * service-defined error code, and request ID, allowing callers to perform more
 * precise error handling and problem diagnosis.
 */
public class ServiceException extends MaxStorageException {

  private static final long serialVersionUID = 1L;

  private final int httpStatus;
  private final String errorCode;
  private final String requestId;

  /**
   * Constructs a new ServiceException.
   *
   * @param httpStatus   HTTP response status code (e.g., 404, 500).
   * @param errorCode    Business error code defined by the service in the response body (e.g., "TableNotFound").
   * @param errorMessage Error description information defined by the service in the response body.
   * @param requestId    Unique ID for tracking this request (usually from the x-odps-request-id header).
   */
  public ServiceException(int httpStatus, String errorCode, String errorMessage, String requestId) {
    // Call the parent constructor to set the main message of the exception
    super(errorMessage);
    this.httpStatus = httpStatus;
    this.errorCode = errorCode;
    this.requestId = requestId;
  }

  public ServiceException(int httpStatus, String errorCode, String errorMessage, String requestId, Throwable cause) {
    // Call the parent constructor to set the main message of the exception
    super(errorMessage, cause);
    this.httpStatus = httpStatus;
    this.errorCode = errorCode;
    this.requestId = requestId;
  }

  /**
   * Gets the HTTP status code.
   *
   * @return HTTP status code.
   */
  public int getHttpStatus() {
    return httpStatus;
  }

  /**
   * Gets the service-defined business error code.
   *
   * @return The service-specific error code.
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * Gets the unique ID for this request.
   *
   * @return The request ID.
   */
  public String getRequestId() {
    return requestId;
  }
  

  @Override
  public String toString() {
    return "ServiceException {" +
           "httpStatus=" + httpStatus +
           ", errorCode='" + errorCode + '\'' +
           ", errorMessage='" + getMessage() + '\'' +
           ", requestId='" + requestId + '\'' +
           '}';
  }
}

