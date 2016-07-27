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

package com.aliyun.odps.udf;

import java.io.IOException;

/**
 * 具有拉数据功能的UDTF，可以主动调用getNextRow()获取一条记录。
 * 仅在LOT中才能使用，并且有如下限制：
 * 1. 在一个stage中只能有一个StandaloneUDTF
 * 2. StandaloneUDTF在LOT的结构中不能有siblings
 */
public abstract class StandaloneUDTF extends UDTF {

  private UDTFPuller puller;

  /**
   * 该方法由框架调用来初始化 StandaloneUDTF
   */
  public final void init(UDTFPuller puller) {
    this.puller = puller;
  }

  /**
   * run方法默认实现，每拉一条记录，调用一次process方法。
   *
   * @throws UDFException
   */
  public void run() throws UDFException,IOException {
    Object[] args;
    while ((args = getNextRow()) != null) {
      process(args);
    }
  }

  /**
   * 获取下一条记录
   *
   * @return
   */
  public final Object[] getNextRow() {
    return puller.getNextRow();
  }

  public final UDTFPuller getPuller() {
    return puller;
  }

}
