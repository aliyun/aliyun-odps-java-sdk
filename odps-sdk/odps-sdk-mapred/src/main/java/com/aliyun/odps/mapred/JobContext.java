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

package com.aliyun.odps.mapred;

import com.aliyun.odps.Column;
import com.aliyun.odps.mapred.conf.JobConf;

/**
 * 作业上下文.
 *
 * <p>
 * JobContext 接口主要定义一些配置参数的获取接口，这些配置参数都是通过 {@link JobConf} 设置的
 * </p>
 */
public interface JobContext {

  /**
   * 获取作业配置.
   *
   * 在 {@link JobConf} 设置的配置参数可以从作业配置中读取
   *
   * <p>
   * 注意：这里只能get配置参数，而不能set配置参数
   * </p>
   *
   * @return 作业配置
   */
  public JobConf getJobConf();

  /**
   * 获取实际运行的{@link Reducer}数, 对于Map-only的作业，Reduce个数为0.
   *
   * 如果通过{@link JobConf#setNumReduceTasks(int)}设定过{@link Reducer}数，则返回值与其相同；
   * 否则，为Map Task数的1/4.
   * 对于pipeline模式的job，返回下一级节点的reducer数目，如果为最后一个reduce节点，则返回该节点的数目，
   * 如果只有map节点，则返回0
   *
   * @return 实际的{@link Reducer} 数.
   * @see JobConf#getNumReduceTasks()
   */
  public int getNumReduceTasks();

  /**
   * 获取Map输出到Reduce的Key类型
   *
   * @return Map输出到Reduce的Key类型
   * @see JobConf#setMapOutputKeySchema(Column[])
   */
  public Column[] getMapOutputKeySchema();

  /**
   * 获取Map输出到Reduce的Value类型
   *
   * @return Map输出到Reduce的Value类型
   * @see JobConf#setMapOutputValueSchema(Column[])
   */
  public Column[] getMapOutputValueSchema();

  /**
   * 获取 {@link Mapper} Java类
   *
   * @return {@link Mapper} Java类
   * @throws ClassNotFoundException
   * @see JobConf#setMapperClass(Class)
   */
  public Class<? extends Mapper> getMapperClass() throws ClassNotFoundException;

  /**
   * 获取 Combiner Java类
   *
   * @return Combiner Java类(Reducer实现)
   * @throws ClassNotFoundException
   * @see JobConf#setCombinerClass(Class)
   */
  public Class<? extends Reducer> getCombinerClass()
      throws ClassNotFoundException;

  /**
   * 获取 {@link Reducer} Java类
   *
   * @return {@link Reducer} Java类
   * @throws ClassNotFoundException
   * @see JobConf#setReducerClass(Class)
   */
  public Class<? extends Reducer> getReducerClass()
      throws ClassNotFoundException;

  /**
   * 获取Map到Reduce输出的Key的分组列，用于对Key/Value记录按Key进行分组.
   * 如果用户没有设置分组列，则默认会使用key的所有列用于分组.
   *
   * @return 分组列
   * @see JobConf#setOutputGroupingColumns(String[])
   */
  String[] getGroupingColumns();
}
