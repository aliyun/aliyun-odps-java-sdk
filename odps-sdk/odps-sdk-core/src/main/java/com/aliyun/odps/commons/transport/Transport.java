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

import com.aliyun.odps.Survey;

/**
 * Transport提供HTTP请求功能
 *
 * <p>
 * Transport提供两种方式发起HTTP请求: <br />
 * 1. 通过{@link Transport#request}发起的请求直接带请求的body数据，<br />
 * 响应的{@code Response}对象包含响应的body数据<br />
 * <br />
 * 2. 通过{@link Transport#connect}发起的请求返回{@link Connection}对象, <br />
 * 请求和响应的body数据需要通过{@link Connection}上的输入和输出流处理<br />
 * </p>
 *
 * @author shenggong.wang@alibaba-inc.com
 */
@Survey
public interface Transport {

  /**
   * 发起HTTP请求
   *
   * <p>
   * 使用{@code request()}发起HTTP请求, 请求的body需要通过{@link Request#setBody}提供, 响应的body数据直接通
   * 过返回的{@link Response#getBody}获取
   * </p>
   *
   * @param req
   *     Request
   * @return Response
   * @throws IOException
   */
  public Response request(Request req) throws IOException;

  /**
   * 发起HTTP请求
   *
   * <p>
   * 使用{@code connect()}发起请求, 请求的响应的body需要通过{@link Connection}来处理,
   * {@code Request} 对象上的body会被忽略, 返回的{@code Response}也获取不到body数据
   * </p>
   *
   * @param req
   * @return
   * @throws IOException
   */
  public Connection connect(Request req) throws IOException;
}
