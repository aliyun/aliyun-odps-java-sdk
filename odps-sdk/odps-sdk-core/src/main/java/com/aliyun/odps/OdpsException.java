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

package com.aliyun.odps;

import com.aliyun.odps.rest.RestException;

/**
 * ODPS SDK产生的异常
 *
 * @author shenggong.wang@alibaba-inc.com
 */
@SuppressWarnings("serial")
public class OdpsException extends Exception {
  protected Integer status;
  protected String requestId;
  protected String errorCode;

  public OdpsException() {

  }

  public OdpsException(String msg) {
    super(msg);
  }

  public OdpsException(String code, String msg) {
    super(msg);
    this.errorCode = code;
  }

  public OdpsException(String msg, Throwable t) {
    super(msg, t);
    if (t instanceof RestException) {
      this.errorCode = ((RestException) t).getErrorMessage().getErrorcode();
      this.requestId = ((RestException) t).getErrorMessage().getRequestId();
    }
  }

  public OdpsException(String code, String msg, Throwable t) {
    super(msg, t);
    this.errorCode = code;
  }

  public OdpsException(Exception e) {
    super(e);
  }

  /**
   * 获取 失败请求的 RequestID, 如果不是网络请求 返回 null
   *
   * @return requestID
   */
  public String getRequestId() {
    return requestId;
  }

  /**
   * 设置 失败请求的 RequestID
   *
   * @param requestId
   */
  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  /**
   * 设置网络状态
   *
   * @param  status
   */
  public void setStatus(Integer status) {
    this.status = status;
  }

  /**
   * 获得网络状态
   *
   * @return status
   */
  public Integer getStatus() {
    return status;
  }

  /**
   * 获取错误码
   *
   * @return err code
   */
  public String getErrorCode() {
    return errorCode;
  }
}
