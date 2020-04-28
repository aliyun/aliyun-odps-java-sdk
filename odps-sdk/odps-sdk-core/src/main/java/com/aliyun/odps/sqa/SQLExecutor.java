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

package com.aliyun.odps.sqa;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;

import java.io.IOException;
import java.util.*;

public interface SQLExecutor {

  /**
   * 默认行为关闭该executor,若为连接池模式,则归还Executor到连接池中
   * @return
   * @throws
   */
  public void close();

  /**
   * 获取Executor的ID

   * @return
   * @throws
   */
  public String getId();

  /**
   * 获取当前query Logview

   * @return
   * @throws
   */
  public String getLogView();

  /**
   * 检查该Executor是否活跃

   * @return 是否存活
   * @throws
   */
  public boolean isActive();

  /**
   * 取消当前查询

   * @return
   * @throws
   */
  public void cancel() throws OdpsException;

  /**
   * 获取当前查询Instance

   * @return
   * @throws
   */
  public Instance getInstance();

  /**
   * 获取当前查询的进度信息

   * @return 各阶段进度信息
   * @throws OdpsException
   */
  public List<Instance.StageProgress> getProgress() throws OdpsException ;

  /**
   * 获取当前查询的执行日志

   * @return 执行信息,包括回退等消息
   * @throws OdpsException
   */
  public List<String> getExecutionLog();

  /**
   * 通过InstanceTunnel获取所有结果

   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  public List<Record> getResult()
      throws OdpsException, IOException;

  /**
   * 通过InstanceTunnel获取结果的迭代器

   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  public ResultSet getResultSet()
      throws OdpsException, IOException;

  /**
   * 通过InstanceTunnel获取有限集结果
   *
   * @param limit
   *     返回结果数量
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  public List<Record> getResult(Long limit)
      throws OdpsException, IOException;

  /**
   * 通过InstanceTunnel获取有限集结果的迭代器

   * @param limit
   *     返回结果数量
   * @return query执行的所有结果
   * @throws OdpsException, IOException
   */
  public ResultSet getResultSet(Long limit)
      throws OdpsException, IOException;

  /**
   * 提交一个query
   *
   * @param sql
   *     sql语句
   * @param hint
   *     query需要的hint参数
   * @return
   * @throws OdpsException
   */
  public void run(String sql, Map<String, String> hint) throws OdpsException;
}