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

package com.aliyun.odps.ml;

/**
 * OfflineModelFilter用于查询所有模型时根据条件过滤
 */
public class OfflineModelFilter {

  private String name;
  private String owner;

  /**
   * 设置模型名前缀
   *
   * @param name
   *     模型名前缀
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * 获得模型名前缀
   *
   * @return 模型名前缀
   */
  public String getName() {
    return this.name;
  }

  /**
   * 获得模型 owner
   *
   * @return 模型 owner
   */
  public String getOwner() {
    return owner;
  }

  /**
   * 设置模型 owner
   *
   * @param owner
   *     模型 owner
   */
  public void setOwner(String owner) {
    this.owner = owner;
  }
}
