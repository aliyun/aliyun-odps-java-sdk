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

package com.aliyun.odps.mapred.utils;

import java.util.Arrays;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.conf.JobConf;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 表达MapReduce任务输入数据的工具类
 */
public class InputUtils {

  private static final Gson gson = new GsonBuilder().create();

  private final static String INPUT_DESC = "odps.mapred.input.desc";

  private final static String VOLUME_INPUT_DESC = "odps.mapred.volume.input.desc";

  /**
   * 添加表table到任务输入
   *
   * @param table
   *     输入表
   * @param conf
   *     作业配置
   */
  public static void addTable(TableInfo table, JobConf conf) {
    TableInfo[] tableInfos = getTables(conf);
    if (tableInfos == null) {
      tableInfos = new TableInfo[1];
    } else {
      tableInfos = Arrays.copyOf(tableInfos, tableInfos.length + 1);
    }
    tableInfos[tableInfos.length - 1] = table;
    conf.set(INPUT_DESC, gson.toJson(tableInfos));
    if (table.getMapperClass() != null
        && conf.get("odps.mapred.map.class") == null) {
      // to make sure odps.mapred.map.class is not empty
      conf.setMapperClass((Class<? extends Mapper>) table.getMapperClass());
    }
  }

  /**
   * 设定表tables为任务输入
   *
   * @param tables
   *     输入表数组
   * @param conf
   *     作业配置
   */
  public static void setTables(TableInfo[] tables, JobConf conf) {
    if (tables != null && tables.length > 0) {
      conf.set(INPUT_DESC, gson.toJson(tables));
    } else {
      conf.set(INPUT_DESC, "");
    }
  }

  /**
   * 获取任务输入表
   *
   * @param conf
   *     作业配置
   * @return 输入表，或null如果没有输入表
   */
  public static TableInfo[] getTables(JobConf conf) {
    String inputDesc = conf.get(INPUT_DESC);
    if (inputDesc != null && !inputDesc.isEmpty()) {
      return gson.fromJson(inputDesc, TableInfo[].class);
    }
    return null;
  }

  public static void addVolume(VolumeInfo volume, JobConf conf) {
    VolumeInfo[] volumeInfos = getVolumes(conf);
    if (volumeInfos == null) {
      volumeInfos = new VolumeInfo[1];
    } else {
      volumeInfos = Arrays.copyOf(volumeInfos, volumeInfos.length + 1);
    }
    volumeInfos[volumeInfos.length - 1] = volume;
    conf.set(VOLUME_INPUT_DESC, gson.toJson(volumeInfos));
  }

  public static void setVolumes(VolumeInfo[] volumes, JobConf conf) {
    if (volumes != null && volumes.length > 0) {
      conf.set(VOLUME_INPUT_DESC, gson.toJson(volumes));
    } else {
      conf.set(VOLUME_INPUT_DESC, "");
    }
  }

  public static VolumeInfo[] getVolumes(JobConf conf) {
    String volumeInputDesc = conf.get(VOLUME_INPUT_DESC);
    if (volumeInputDesc != null && !volumeInputDesc.isEmpty()) {
      return gson.fromJson(volumeInputDesc, VolumeInfo[].class);
    }
    return null;
  }
}
