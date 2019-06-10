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

package com.aliyun.odps.udt;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.udf.ExecutionContext;

/**
 * UDT运行时的执行上下文信息。
 * <p>
 * 是 com.aliyun.odps.udf.ExecutionContext 的子类
 * </p>
 */
public abstract class UDTExecutionContext extends ExecutionContext {

  private static final ThreadLocal<UDTExecutionContext> contexts = new ThreadLocal<UDTExecutionContext>();

  /**
   * 获取当前的UDT上下文信息。在UDT执行之前，由UDT执行框架初始化。
   */
  public static UDTExecutionContext get() {
    return contexts.get();
  }

  /**
   * 设置UDT执行的上下文信息。由框架调用。
   */
  static void set(UDTExecutionContext context) {
    contexts.set(context);
  }

  /**
   * 使用默认序列化器，将对象序列化为Binary，用以落盘。
   */
  public abstract Binary serialize(Object obj);

  /**
   * 使用默认反序列化器，将serialize序列化后的内容反序列化成原来类的对象。
   * 注意如果这里再次读入的时候和序列化的时候引用的jar包不一致，可能会导致反序列化失败。
   */
  public abstract Object deserialize(Binary data);
}
