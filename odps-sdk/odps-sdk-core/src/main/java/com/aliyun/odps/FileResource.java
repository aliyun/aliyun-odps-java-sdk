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
 * FileResource表示ODPS中一个文件类型资源
 *
 * @author shenggong.wang@alibaba-inc.com
 */
public class FileResource extends Resource {

  /**
   * FileResource表示ODPS中一个File类型的资源
   */
  public FileResource() {
    super();
    model.type = Type.FILE.toString();
  }

  /**
   * 获得资源在ODPS中记录的MD5值
   *
   * @return 资源的MD5值
   */
  String getContentMd5() {
    if (model.contentMD5 == null && client != null) {
      lazyLoad();
    }
    return model.contentMD5;
  }

  /**
   * 获得资源是否是临时资源。所谓临时资源即可以将本地文件作为{@link Resource}上传到ODPS中，供MapReduce使用。该资源仅对
   * 运行的作业有效。
   *
   * @return 是否是临时资源
   */
  public boolean getIsTempResource() {
    return model.isTempResource;
  }

  /**
   * 设置是否是临时资源
   *
   * @param isTempResource
   *     是否是临时资源
   */
  public void setIsTempResource(boolean isTempResource) {
    model.isTempResource = isTempResource;
  }

}
