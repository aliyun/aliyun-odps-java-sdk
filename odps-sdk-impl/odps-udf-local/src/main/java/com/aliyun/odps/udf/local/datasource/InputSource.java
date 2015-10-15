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

package com.aliyun.odps.udf.local.datasource;

import java.io.IOException;

public abstract class InputSource {

  /**
   * 添加输入源时会调用该方法进行初始化，如：打开文件句柄，打开数据库连接等
   * 
   * @throws IOException
   */
  public void setup() throws IOException{};

  /**
   * 每次调用该方法返回一条记录，Object对象类型只能为：Long,Double,Boolean,Datetime,String
   * @return
   * @throws IOException
   */
  public abstract Object[] getNextRow() throws IOException;

  /**
   * 该方法用于释放资源，如：关闭文件句柄，关闭数据库连接
   * 程序退出之前（正常退出或抛出异常）会调用该方法，不应该抛出异常
   */
  public void close(){};

}
