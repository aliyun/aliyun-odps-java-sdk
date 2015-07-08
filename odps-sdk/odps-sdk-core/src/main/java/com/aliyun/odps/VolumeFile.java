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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * VolumeFile表示ODPS中的volume file
 * <b>暂未开放，仅限内部使用<b/>
 *
 * @author lu.lu@alibaba-inc.com
 */
public class VolumeFile {

  @XmlRootElement(name = "VolumeFileModel")
  static class VolumeFileModel {

    @XmlElement(name = "Name")
    String name;
  }

  private VolumeFileModel model;

  VolumeFile(VolumeFileModel model) {
    this.model = model;
  }

  /**
   * 获得VolumeFile的名字
   *
   * @return VolumeFile名
   */
  public String getName() {
    return model.name;
  }
};
