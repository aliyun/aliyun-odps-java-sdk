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

package com.aliyun.odps.commons.transport;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

/**
 * Response表示HTTP请求的响应
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public abstract class Response {

  protected int status = 500;
  protected String message = null;
  protected Map<String, String> headers = new HashMap<String, String>();
  protected byte[] body;

  /**
   * 检查HTTP请求是否成功
   *
   * <p>
   * HTTP返回值2xx为成功状态
   * </p>
   *
   * @return HTTP返回值2xx: true, 其他: false
   */
  public boolean isOK() {
    return status / 100 == 2;
  }

  /**
   * 获得响应的状态码
   *
   * 状态值参见{@link HttpURLConnection}
   *
   * @return
   */
  public int getStatus() {
    return status;
  }

  /**
   * 获得响应的状态信息
   *
   * 状态信息参见{@link HttpURLConnection}
   *
   * @return
   */
  public String getMessage() {
    return message;
  }

  /**
   * 获得响应的HTTP头
   *
   * @return HTTP头
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  /**
   * 获得响应的HTTP头
   *
   * @param name
   * @return
   */
  public String getHeader(String name) {
    return headers.get(name);
  }

  /**
   * 获得响应的body部分
   *
   * @return {@link Connection#getResponse()}得到的对象返回null, 需要通过{@link
   * Connection#getInputStream()}读取body数据
   */
  public byte[] getBody() {
    return body;
  }
}
