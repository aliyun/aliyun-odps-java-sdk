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

package com.aliyun.odps;

/**
 * VolumeFilter用于查询所有Volume或partition时根据条件过滤Volume或partition
 * <b>暂未开放，仅限内部使用<b/>
 *
 * <p>
 *
 * 注: Volume名是指Volume名的前缀 <br />
 * 例如:<br />
 *
 * <pre>
 * <code>
 * VolumeFilter filter = new VolumeFilter();
 * filter.setName("my_volume_prefix");
 *
 * for (Volume t : odps.volumes().iterator(filter)) {
 *     // do somthing on the Volume object
 * }
 * </code>
 * </pre>
 * </p>
 *
 * @author lu.lu@alibaba-inc.com
 */
public class VolumeFilter {

  private String name;

  /**
   * 设置Volume名或partition名前缀
   *
   * @param name
   *     Volume名或partition名前缀
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * 获得Volume名或partition名前缀
   *
   * @return Volume名或partition名前缀
   */
  public String getName() {
    return name;
  }
}
