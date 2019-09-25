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
 * UDF 基类
 * UDF (User Defined Scalar Function) 自定义函数，其输入输出是一对一的关系，即读入一行数据，写出一条输出值。
 * <p>
 * 自定义 UDF 需要继承这个类来实现一个 UDF，并根据输入输出类型编写 evaluate 方法。
 * </p>
 * 例如:
 * <pre>
 *   public class MyPlus extends UDF {
 *     Long evaluate(Long a, Long b) {
 *       if (a == null || b == null) {
 *         return null;
 *       }
 *       return a + b;
 *     }
 *   }
 * </pre>
 */
public class UDF implements ContextFunction {

  @Override
  public void setup(ExecutionContext ctx) throws UDFException, IOException {
  }

  @Override
  public void close() throws UDFException, IOException {
  }
}
