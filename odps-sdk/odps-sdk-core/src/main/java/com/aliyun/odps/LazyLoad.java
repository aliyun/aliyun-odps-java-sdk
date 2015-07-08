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
 * LazyLoad表示此类型的属性值可能是延迟加载的
 *
 * <p>
 * 延迟加载的含义是, 对象的属性值可能不存在，在调用gets等方法时视需要通过RESTful API从服务器断获取
 * </p>
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public abstract class LazyLoad {

  private boolean loaded;

  /**
   * 判断属性是否已经加载
   *
   * @return 如果对象已经被标记为已加载返回true, 否则返回false
   */
  boolean isLoaded() {
    return loaded;
  }

  /**
   * 标记属性已经加载
   *
   * @param loaded
   */
  protected void setLoaded(boolean loaded) {
    this.loaded = loaded;
  }

  /**
   * 加载属性
   *
   * <p>
   * 如果状态为已加载则忽略
   * </p>
   */
  protected void lazyLoad() {
    if (!loaded) {
      try {
        reload();
      } catch (OdpsException e) {
        throw new ReloadException(e.getMessage(), e);
      }
    }
  }

  /**
   * 重新加载属性值
   *
   * @throws OdpsException
   */
  public abstract void reload() throws OdpsException;
}
