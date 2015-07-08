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

package com.aliyun.odps.account;

import com.aliyun.odps.commons.transport.Request;

/**
 * RequestSigner用于对HTTP请求进行签名
 *
 * <p>
 * 不同的账号类型可能使用不同的签名机制或算法,{@link Account}的实现类一般是RequestSigner的工厂。
 * </p>
 */
public interface RequestSigner {

  /**
   * 对HTTP请求签名
   *
   * <p>
   * 第一个参数resource表示RESTful资源标识
   * </p>
   *
   * @param resource
   *     资源标识, 如: /projects/my_project/tables/my_table
   * @param req
   *     {@link Request}
   */
  public void sign(String resource, Request req);
}
