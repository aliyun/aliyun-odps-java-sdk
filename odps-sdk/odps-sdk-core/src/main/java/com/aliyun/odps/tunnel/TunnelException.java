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

package com.aliyun.odps.tunnel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * 该异常在DataTunnel服务访问失败时抛出。
 */
@SuppressWarnings("serial")
public class TunnelException extends OdpsException {

  private String errorMsg;

  /**
   * 构造异常对象
   */
  public TunnelException() {
  }

  public TunnelException(String requestId, InputStream in, Integer status) {
    String message = "";
    try {
      byte[] bytes = IOUtils.readFully(in);
      message = new String(bytes);
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      loadFromJson(bis);
    } catch (Exception e) {
      if (StringUtils.isNullOrEmpty(message)) {
        message = "Error message not available";
      }
      this.errorMsg = message;
      this.errorCode = TunnelConstants.LOCAL_ERROR_CODE;
    }
    this.requestId = requestId;
    this.status = status;
  }

  /**
   * 构造异常对象
   *
   * @param message
   */
  public TunnelException(String message) {
    this(null, message, null);
  }

  /**
   * 构造异常对象
   *
   * @param message
   * @param cause
   */
  public TunnelException(String message, Throwable cause) {
    this(null, message, cause);
  }

  /**
   * 构造异常对象
   *
   * @param requestId
   * @param message
     */
  public TunnelException(String requestId, String message) {
    this(requestId, message, null);
  }

  /**
   * 构造异常对象
   *
   * @param requestId
   * @param message
   * @param cause
     */
  public TunnelException(String requestId, String message, Throwable cause) {
    super(message, cause);
    this.requestId = requestId;
    this.errorCode = TunnelConstants.LOCAL_ERROR_CODE;
    this.errorMsg = message;
  }

  /**
   * 构造异常对象
   *
   * @param is
   */
  public TunnelException(InputStream is) {
    this(null, is, null);
  }

  @Override
  public String getMessage() {
    StringBuffer sb = new StringBuffer();
    if (requestId != null) {
      sb.append("RequestId=").append(requestId);
    }
    if (errorCode != null) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append("ErrorCode=").append(errorCode);
    }
    if (errorMsg != null) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      if (requestId != null || errorCode != null) {
        sb.append("ErrorMessage=");
      }
      sb.append(errorMsg);
    }
    return sb.toString();
  }
  /**
   * 获得错误信息
   *
   * @return
   */
  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public void setErrorMsg(String errorMsg) {
    this.errorMsg = errorMsg;
  }

  @Deprecated
  /**
   * 之后的版本将作为内部方法类
   * 使用构造函数替代
   *
   * @param is
   * @throws TunnelException
   * @throws IOException
   */
  public void loadFromJson(InputStream is) throws TunnelException, IOException {
    try {
      String json = IOUtils.readStreamAsString(is);
      JsonObject tree = new JsonParser().parse(json).getAsJsonObject();
      String node = null;
      if (tree.has("Code")) {
        node = tree.get("Code").getAsString();
        errorCode = node;
      }

      if (tree.has("Message")) {
        node = tree.get("Message").getAsString();
        errorMsg = node;
      }
    } catch (Exception e) {
      throw new TunnelException("Parse response failed", e);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  @Override
  public String toString() {
    return getMessage();
  }

}
