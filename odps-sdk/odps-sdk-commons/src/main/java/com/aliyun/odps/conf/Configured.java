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

package com.aliyun.odps.conf;

/**
 * 提供 {@link Configurable} 接口的基类实现.
 */
public class Configured implements Configurable {

  private Configuration OdpsConf;

  /**
   * 默认构造函数.
   */
  public Configured() {
    this(null);
  }

  /**
   * 构造函数，给定一个 {@link Configuration} 对象.
   *
   * @param conf
   */
  public Configured(Configuration conf) {
    setConf(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.OdpsConf = conf;
  }

  @Override
  public Configuration getConf() {
    return OdpsConf;
  }

}
