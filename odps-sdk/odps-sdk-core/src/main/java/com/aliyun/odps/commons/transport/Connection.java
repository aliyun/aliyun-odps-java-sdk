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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Connection表示一次HTTP请求所使用的连接
 *
 * <p>
 * 一个Connection对象与一次HTTP请求的生命周期相同, 当请求结束后Connection不再可用。<br />
 *
 * 一次HTTP请求有如下过程: <br />
 * 1. 通过{@link connect(Request req)}方法发起HTTP请求, HTTP头会发送<br />
 * 2. 通过{@link getOutputStream()}获得输出流，发送HTTP body数据 <br />
 * 3. {@link getResponse()}获得HTTP响应，包含状态吗, HTTP头 <br />
 * 4. {@link getInputStream()}读取HTTP响应的body部分数据 <br />
 * 5.{@link disconnect()}请求结束后释放资源
 * </p>
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public interface Connection {

  /**
   * 发起HTTP请求
   *
   * @param req
   *     HTTP请求
   * @throws IOException
   */
  public void connect(Request req) throws IOException;

  /**
   * 获得HTTP连接上的流,以写入HTTP请求的body
   *
   * @return OutputStream
   * @throws IOException
   */
  public OutputStream getOutputStream() throws IOException;

  /**
   * 获得HTTP请求的响应
   *
   * @return Response对象
   * @throws IOException
   */
  public Response getResponse() throws IOException;

  /**
   * 获得HTTP连接上的流,以读取HTTP请求响应的body部分数据
   *
   * @return InputStream
   * @throws IOException
   */
  public InputStream getInputStream() throws IOException;

  /**
   * 关闭HTTP连接
   *
   * @throws IOException
   */
  public void disconnect() throws IOException;
}
