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

package com.aliyun.odps.task.copy;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.commons.util.TrimmedStringXmlAdapter;

@XmlType(name = "",
    propOrder = {"type", "project", "table", "partition"})
public class Datasource {

  public static enum Direction {
    IMPORT, EXPORT
  }

  @XmlElement(name = "Type")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  private String type;

  @XmlElement(name = "Project")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  private String project;

  @XmlElement(name = "Table")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  private String table;

  @XmlElement(name = "Partition")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  private String partition;

  Datasource() {

  }

  public Datasource(String type, String project, String table,
                    String partition) {
    this.type = type;
    this.project = project;
    this.table = table;
    this.partition = partition;
  }

  public String getProject() {
    return project;
  }

  public String getTable() {
    return table;
  }

  public String getPartition() {
    return partition;
  }

  public String getType() {
    return type;
  }
}

