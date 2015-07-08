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

/**
 *
 */
package com.aliyun.odps.task;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.Task;
import com.aliyun.odps.commons.util.TrimmedStringXmlAdapter;

/**
 * 执行SQL Plan的任务。
 *
 * @author xiaoming.yin
 */
@XmlRootElement(name = "SQLPlan")
public class SqlPlanTask extends Task {

  SqlPlanTask() {
  }

  private String query;

  /**
   * 使用给定任务名构造一个{@link SqlPlanTask}实例。
   *
   * @param name
   *     任务名。
   */
  public SqlPlanTask(String name) {
    this(name, null);
  }

  /**
   * 使用给定任务名和查询语句构造一个{@link SqlPlanTask}实例。
   *
   * @param name
   *     任务名。
   * @param query
   *     查询语句。
   */
  public SqlPlanTask(String name, String query) {
    super();
    setName(name);
    this.query = query;
  }

  /**
   * 返回查询语句。
   *
   * @return 查询语句。
   */
  public String getQuery() {
    return query;
  }

  /**
   * 设置查询语句。
   *
   * @param query
   *     查询语句。
   */
  @XmlElement(name = "Query")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  public void setQuery(String query) {
    this.query = query;
  }

  public String getCommandText() {
    return query;
  }
}
