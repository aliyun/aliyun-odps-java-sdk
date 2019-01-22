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

package com.aliyun.odps.mapred.bridge;

import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableUtils;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.udf.StandaloneUDTF;
import com.aliyun.odps.udf.UDFException;

public abstract class LotTaskUDTF extends StandaloneUDTF {

  protected BridgeJobConf conf;
  protected Column[] inputSchema;

  public LotTaskUDTF() {
    URL url = this.getClass().getClassLoader().getResource("jobconf.xml");
    if (url == null) {
      URL[] urls = ((URLClassLoader) this.getClass().getClassLoader()).getURLs();
      throw new RuntimeException("Job configure file jobconf.xml not found. Classpath:"
                                 + StringUtils.join(urls, ":"));
    }
    conf = new BridgeJobConf(false);
    conf.addResource("jobconf.xml");
  }

  public LotTaskUDTF(String functionName) {
    this();
    conf.set("odps.mr.sql.functionName", functionName);
  }

  /**
   * A hack to work around with protected method forward(...)
   *
   * @param objs
   */
  public void collect(Object[] objs) {
    try {
      forward(objs);
    } catch (UDFException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deep copy of writable objects. Not that different from primative types, the
   * references within the writable array are also reused.
   *
   * @param objs
   * @return object array
   */
  protected Object[] writableDeepCopy(Object[] objs) {
    Writable[] rt = new Writable[objs.length];
    for (int i = 0; i < objs.length; i++) {
      if (objs[i] != null) {
        rt[i] = (Writable) WritableUtils.clone((Writable) objs[i], conf);
      }
    }
    return rt;
  }

  @Override
  public void process(Object[] unused) throws UDFException {
  }
}
