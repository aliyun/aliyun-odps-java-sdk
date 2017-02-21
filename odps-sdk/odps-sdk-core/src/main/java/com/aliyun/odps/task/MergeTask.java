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
 * 表示执行一个Merge查询的任务。
 *
 * @author fengyin.zym
 */
@XmlRootElement(name = "Merge")
public class MergeTask extends Task {

  private String table;

  // Package-visible. Only for JAXB to construct the instance.
  MergeTask() {
  }

  /**
   * 使用给定任务名构造一个{@link MergeTask}实例。
   *
   * @param name
   *     任务名。
   */
  public MergeTask(String name) {
    this(name, null);
  }

  /**
   * 使用给定任务名和查询语句构造一个{@link MergeTask}实例。
   *
   * @param name
   *     任务名。
   * @param tb
   *     表、分区信息。
   */
  public MergeTask(String name, String table) {
    super();
    setName(name);
    this.table = table;
  }

  /**
   * 返回查询语句。
   *
   * @return 查询语句。
   */
  public String getTable() {
    return table;
  }

  /**
   * 设置查询语句。
   *
   * @param query
   *     查询语句。
   */
  @XmlElement(name = "TableName")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  public void setTable(String table) {
    this.table = table;
  }
}
