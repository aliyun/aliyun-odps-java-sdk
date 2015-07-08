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
 * OdpsHook 对象，用于在提交作业前后插入代码
 */
public abstract class OdpsHook {

  /**
   * 提交代码前，框架会将 Job 和 Odps 对象传入 Hook
   *
   * @param job
   * @param odps
   * @throws OdpsException
   */
  public abstract void before(Job job, Odps odps) throws OdpsException;


  /**
   * instance 结束后，框架会将 Instance 和 odps 对象传入 hook
   *
   * @param instance
   * @param odps
   * @throws OdpsException
   */
  public abstract void after(Instance instance, Odps odps) throws OdpsException;
}
