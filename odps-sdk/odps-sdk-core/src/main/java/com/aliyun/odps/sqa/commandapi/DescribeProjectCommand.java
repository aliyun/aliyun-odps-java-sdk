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

package com.aliyun.odps.sqa.commandapi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class DescribeProjectCommand implements Command {

  private static final Map<String, TypeInfo> projectMap = new LinkedHashMap<>();

  static {
    projectMap.put("Name", TypeInfoFactory.STRING);
    projectMap.put("Comment", TypeInfoFactory.STRING);
    projectMap.put("Type", TypeInfoFactory.STRING);
    projectMap.put("Owner", TypeInfoFactory.STRING);
    projectMap.put("CreatedTime", TypeInfoFactory.DATETIME);
    projectMap.put("Properties",
                   TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.STRING));
  }

  private static final Map<String, TypeInfo> projectExtendedMap = new LinkedHashMap<>(projectMap);

  static {
    projectExtendedMap.put("ExtendedProperties", TypeInfoFactory
        .getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.STRING));
  }

  private String projectName;
  private boolean extended = false;

  public DescribeProjectCommand(String projectName, boolean extended) {
    this.projectName = projectName;
    this.extended = extended;
  }

  @Override
  public boolean isSync() {
    return true;
  }

  @Override
  public List<String> getResultHeaders() {
    return new ArrayList<>(getResultTypeInfo().keySet());
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return new ArrayList<>(getResultTypeInfo().values());
  }

  private Map<String, TypeInfo> getResultTypeInfo() {
    if (extended) {
      return projectExtendedMap;
    }
    return projectMap;
  }

  @Override
  public RecordIter<Record> run(Odps odps, CommandInfo commandInfo) throws OdpsException {

    Project prj = odps.projects().get(projectName);
    prj.reload();

    Map<String, Object> map = new HashMap<>();
    map.put("Name", prj.getName());
    map.put("Comment", prj.getComment());
    map.put("Type", prj.getType().toString());
    map.put("Owner", prj.getOwner());
    map.put("CreatedTime", prj.getCreatedTime());
    map.put("Properties", prj.getProperties());
    if (extended) {
      // project的物理大小，不进行可读化转换
      map.put("ExtendedProperties", prj.getExtendedProperties());
    }

    return new RecordIter<>(CommandUtil.toRecord(map, getResultTypeInfo()).iterator(),
                            getResultHeaders(), getResultTypes());
  }
}
