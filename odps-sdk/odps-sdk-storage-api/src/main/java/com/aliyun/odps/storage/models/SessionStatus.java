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

package com.aliyun.odps.storage.models;

import com.aliyun.odps.storage.ClientException;

/**
 * Enumeration of session statuses for read/write operations.
 *
 * <p>This enum defines the different states a session can be in during its lifecycle.
 * Sessions are created in INIT state and transition to NORMAL when ready for use.
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public enum SessionStatus {
  /**
   * Session is being initialized.
   */
  INIT,
  /**
   * Session is ready for read/write operations.
   */
  NORMAL,
  /**
   * Session is being committed.
   */
  COMMITTING,
  /**
   * Session has been committed successfully.
   */
  COMMITTED,
  /**
   * Session is in a critical state.
   */
  CRITICAL,
  /**
   * Session has expired.
   */
  EXPIRED,
  /**
   * Unknown session status.
   */
  UNKNOWN;

  /**
   * Converts a string to SessionStatus.
   *
   * @param status The string representation of the session status
   * @return The corresponding SessionStatus enum value
   * @throws ClientException if the status string is not recognized
   */
  public static SessionStatus fromString(String status) {
    if (status == null) {
      return UNKNOWN;
    }
    switch (status.toUpperCase()) {
      case "INIT":
        return INIT;
      case "NORMAL":
        return NORMAL;
      case "COMMITTING":
        return COMMITTING;
      case "COMMITTED":
        return COMMITTED;
      case "CRITICAL":
        return CRITICAL;
      case "EXPIRED":
        return EXPIRED;
      default:
        return UNKNOWN;
    }
  }

  /**
   * Converts SessionStatus to string.
   *
   * @return The string representation of the session status
   */
  @Override
  public String toString() {
    switch (this) {
      case INIT:
        return "INIT";
      case NORMAL:
        return "NORMAL";
      case COMMITTING:
        return "COMMITTING";
      case COMMITTED:
        return "COMMITTED";
      case CRITICAL:
        return "CRITICAL";
      case EXPIRED:
        return "EXPIRED";
      default:
        return "UNKNOWN";
    }
  }
}