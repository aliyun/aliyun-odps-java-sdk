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

import java.util.ArrayList;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.codehaus.jackson.JsonNode;

/**
 * Created by yinyue on 15-3-17.
 */
public final class Shard {

  @Deprecated
  private long hubLifecycle;

  private long shardNum;
  private ArrayList<String> distributeCols;
  private ArrayList<String> sortCols;

  Shard() {
    hubLifecycle = -1;
  }

  public static Shard parseShard(JSONObject tree) {
    try {
      Shard shard = new Shard();

      Long node = tree.getLong("ShardNum");
      if (node != null) {
        shard.setShardNum(node);
      }

      node = tree.getLong("HubLifecycle");
      if (node != null) {
        shard.setHubLifecycle(node);
      }

      JSONArray distributeCols = tree.getJSONArray("DistributeCols");
      if (distributeCols != null) {
        ArrayList<String> shardDistributeCols = new ArrayList<String>();
        for (int i = 0; i < distributeCols.size(); ++i) {
          String col = distributeCols.getString(i);
          shardDistributeCols.add(col);
        }
        shard.setDistributeColumnNames(shardDistributeCols);
      }

      JSONArray sortCols = tree.getJSONArray("SortCols");
      if (sortCols != null) {
        ArrayList<String> shardSortCols = new ArrayList<String>();
        for (int i = 0; i < sortCols.size(); ++i) {
          String col = sortCols.getString(i);
          shardSortCols.add(col);
        }
        shard.setSortColumnNames(shardSortCols);
      }

      return shard;

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
  
  /**
   * @deprecated use another method parseShard(JSONObject tree)
   */
  public static Shard parseShard(JsonNode tree) {
    try {
      Shard shard = new Shard();

      JsonNode node = tree.get("ShardNum");
      if (node != null && !node.isNull()) {
        shard.setShardNum(node.asLong());
      }

      node = tree.get("HubLifecycle");
      if (node != null && !node.isNull()) {
        shard.setHubLifecycle(node.asLong());
      }

      JsonNode distributeCols = tree.get("DistributeCols");
      if (distributeCols != null && !distributeCols.isNull() && distributeCols.isArray()) {
        ArrayList<String> shardDistributeCols = new ArrayList<String>();
        for (JsonNode col : distributeCols) {
          shardDistributeCols.add(col.asText());
        }
        shard.setDistributeColumnNames(shardDistributeCols);
      }

      JsonNode sortCols = tree.get("SortCols");
      if (sortCols != null && !sortCols.isNull() && sortCols.isArray()) {
        ArrayList<String> shardSortCols = new ArrayList<String>();
        for (JsonNode col : sortCols) {
          shardSortCols.add(col.asText());
        }
        shard.setSortColumnNames(shardSortCols);
      }

      return shard;

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * 返回表的HubLifecycle 单位：天
   *
   * @return lifecycle， 如果表不为hub表返回 -1
   */
  @Deprecated
  public long getHubLifecycle() {
    return hubLifecycle;
  }

  /**
   * 返回shard数量
   *
   * @return shard数
   */
  public long getShardNum() {
    return shardNum;
  }

  /**
   * 返回创建shard表时指定的cluster by列名
   *
   * @return 列名数组, 不存在则返回空数组
   */
  public ArrayList<String> getDistributeColumnNames() {
    return distributeCols;
  }

  /**
   * 返回创建shard表时指定的sorted by列名
   *
   * @return 列名数组，不存在则返回空数组
   */
  public ArrayList<String> getSortColumnNames() {
    return sortCols;
  }

  @Deprecated
  private void setHubLifecycle(long lifecycle) {
    hubLifecycle = lifecycle;
  }

  private void setShardNum(long num) {
    shardNum = num;
  }

  private void setDistributeColumnNames(ArrayList<String> columnNames) {
    distributeCols = columnNames;
  }

  private void setSortColumnNames(ArrayList<String> columnNames) {
    sortCols = columnNames;
  }
}
