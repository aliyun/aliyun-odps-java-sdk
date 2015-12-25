/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aliyun.odps;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * Volume Exception
 * <p>
 * In order to unify {@link TunnelException} and {@link OdpsException}
 * </p>
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
@SuppressWarnings("serial")
public class VolumeException extends Exception {

  private String requestId;

  private String errCode;

  private String errMsg;

  public static final String UNKNOWN_REQUEST_ID = "Unknown";

  public static final String NON_VOLUME_FS_ERROR = "NonVolumeFSError";

  public VolumeException(Throwable e) {
    super(e);
    if (e != null) {
      if (e instanceof TunnelException) {
        TunnelException te = (TunnelException) e;
        errCode = te.getErrorCode();
        errMsg = te.getErrorMsg();
        requestId = te.getRequestId();
      } else if (e instanceof OdpsException) {
        OdpsException oe = (OdpsException) e;
        errCode = oe.getErrorCode();
        errMsg = oe.getMessage();
        requestId = oe.getRequestId();
      } else {
        requestId = UNKNOWN_REQUEST_ID;
        errMsg = e.getMessage();
        errCode = NON_VOLUME_FS_ERROR;
      }
    }
  }

  public VolumeException(String errMsg, Throwable e) {
    this(e);
    this.errMsg = errMsg;
  }

  public VolumeException(String errCode, String errMsg) {
    this.requestId = UNKNOWN_REQUEST_ID;
    this.errCode = errCode;
    this.errMsg = errMsg;
  }
  
  public VolumeException(String errMsg) {
    super(errMsg);
    this.requestId = UNKNOWN_REQUEST_ID;
    this.errCode = NON_VOLUME_FS_ERROR;
    this.errMsg = errMsg;
  }

  @Override
  public String getMessage() {
    StringBuffer sb = new StringBuffer();
    if (requestId != null) {
      sb.append("RequestId=").append(requestId);
    }
    if (errCode != null) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("ErrCode=").append(errCode);
    }
    if (errMsg != null) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      if (requestId != null || errCode != null) {
        sb.append("ErrMessage=");
      }
      sb.append(errMsg);
    }
    return sb.toString();
  }


  @Override
  public String toString() {
    return getMessage();
  }

  public String getRequestId() {
    return requestId;
  }

  public String getErrCode() {
    return errCode;
  }

  public String getErrMsg() {
    return errMsg;
  }

}
