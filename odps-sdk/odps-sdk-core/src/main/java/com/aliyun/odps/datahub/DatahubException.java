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

package com.aliyun.odps.datahub;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.util.IOUtils;

/**
 * 该异常在DataHub服务访问失败时抛出。
 */
@SuppressWarnings("serial")
public class DatahubException extends OdpsException {

  private String requestId;
  private String errorCode;
  private String errorMsg;

  /**
   * 构造异常对象
   */
  public DatahubException() {
  }

  /**
   * 构造异常对象
   *
   * @param message
   */
  public DatahubException(String message) {
    super(message);
    this.errorCode = DatahubConstants.LOCAL_ERROR_CODE;
    this.errorMsg = message;
  }

  /**
   * 构造异常对象
   *
   * @param message
   * @param cause
   */
  public DatahubException(String message, Throwable cause) {
    super(message, cause);
    this.errorCode = DatahubConstants.LOCAL_ERROR_CODE;
    this.errorMsg = message;
  }

  /**
   * 构造异常对象
   *
   * @param is
   */
  public DatahubException(InputStream is) {
    String message = "";
    try {
      byte[] bytes = IOUtils.readFully(is);
      message = new String(bytes);
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      loadFromJson(bis);
    } catch (Exception e) {
      throw new RuntimeException("Parse responsed failed: '" + message + "'", e);
    }
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

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  /**
   * 获得请求标识
   *
   * @param requestId
   */
  public String getRequestId() {
    return requestId;
  }

  /**
   * 获得错误代码
   */
  public String getErrorCode() {
    return errorCode;
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


  /**
   * @param is
   * @throws DatahubException
   * @throws IOException
   */
  private void loadFromJson(InputStream is) throws DatahubException, IOException {
    try {
      String json = IOUtils.readStreamAsString(is);
      JSONObject tree = JSON.parseObject(json);
      String code = tree.getString("Code");
      if (code != null) {
        errorCode = code;
      }

      String msg = tree.getString("Message");
      if (msg != null) {
        errorMsg = msg;
      }
    } catch (Exception e) {
      throw new DatahubException("Parse response failed", e);
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
