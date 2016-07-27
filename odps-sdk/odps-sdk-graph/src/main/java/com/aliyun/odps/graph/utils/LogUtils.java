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

package com.aliyun.odps.graph.utils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.graph.counters.MemoryCounter;
import com.aliyun.odps.graph.counters.StatsCounter;
import com.aliyun.odps.utils.StringUtils;

/**
 * Graph作业LOG日志工具类
 */
public class LogUtils {

  /**
   * Graph作业执行阶段枚举
   */
  public static enum GraphStage {
    GRAPH_STAGE_WAIT_WORKER_UP("WAIT_WORKER_UP", 0), //
    GRAPH_STAGE_INIT_NETWORK("INIT_NETWORK", 1), //
    GRAPH_STAGE_LOAD("LOAD", 2), //
    GRAPH_STAGE_SETUP("SETUP", 3), //
    GRAPH_STAGE_SUPERSTEP("SUPERSTEP", 4), //
    GRAPH_STAGE_CHECKPOINT("CHECKPOINT", 5), //
    GRAPH_STAGE_FAILOVER("FAILOVER", 6), //
    GRAPH_STAGE_CLEANUP("CLENAUP", 7), //
    GRAPH_STAGE_FAIL("FAIL", 8), //
    GRAPH_STAGE_TERMINATE("TERMINATE", 9), //
    GRAPH_STAGE_WAIT_SUBMIT("WAIT_SUBMIT", 10), //
    GRAPH_STAGE_WAIT_GET_RESULT("WAIT_GET_RESULT", 11); //

    private String name;
    private int value;

    GraphStage(String name, int value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return this.name;
    }

    public int getValue() {
      return this.value;
    }
  }


  /**
   * 显示Graph作业的summary信息
   *
   * @param ts
   *     TaskSummary对象
   */
  public static void showSummary(TaskSummary ts) {
    if (!StringUtils.isNullOrEmpty(ts.getSummaryText())) {
      System.err.println("Summary:");
      System.err.println(ts.getSummaryText());
    }
  }

  /**
   * 将summary中信息填充到Counters中，并且展示summary
   *
   * @param ts
   *     TaskSummary 对象
   * @param counters
   *     Counters 对象
   * @throws OdpsException
   */
  @SuppressWarnings("unchecked")
  public static void fillCountersAndShowSummary(TaskSummary ts,
                                                Counters counters) throws OdpsException {

    String summary = ts.toString();
    if (!StringUtils.isNullOrEmpty(summary)) {

      Set<Entry<String, Map<String, Object>>> groups = ts.entrySet();
      for (Entry<String, Map<String, Object>> entry : groups) {
        String group = entry.getKey();

        if (!(entry.getValue() instanceof Map)) {
          continue;
        }

        Map<String, Object> groupValues = entry.getValue();
        for (Entry<String, Object> counterItem : groupValues.entrySet()) {
          // counter value in tasksummary may be Integer or Long
          // parse counter value to long
          String counterName = counterItem.getKey();
          Object counterRawValue = counterItem.getValue();
          long counterValue = 0;
          if (counterRawValue instanceof Integer) {
            counterValue = ((Integer) counterRawValue).longValue();
          } else if (counterRawValue instanceof Long) {
            counterValue = (Long) counterRawValue;
          } else if (counterRawValue instanceof BigInteger) {
            counterValue = ((BigInteger) counterRawValue).longValue();
          } else {
            throw new OdpsException("Invalid counter value type: " + counterRawValue.getClass());
          }
          counters.findCounter(group, counterName).setValue(counterValue);

        }
      }
      showSummary(ts);
    }
    if (counters.countCounters() == 0) {
      throw new OdpsException("Get task summary and counters failed");
    }
  }

  static void addIfExists(JSONObject group, String counterName,
                          String key, StringBuilder progress) {
    if (group.containsKey(counterName)) {
      progress.append(key + "=");
      String value = group.getString(counterName);
      if (counterName.equals(MemoryCounter.MAX_USED_MEMORY.toString())) {
        value = String.valueOf(Long.parseLong(value) / 1000000) + "M";
      }
      progress.append(value);
      progress.append(",");
    }
  }


  public static String assembleProgress(TaskSummary ts) throws IOException {

    StringBuilder progress = new StringBuilder();

    if (ts == null) {
      return progress.toString();
    }

    JSONObject jsonSummary = JSON.parseObject(ts.getJsonSummary());

    if (jsonSummary.containsKey(StatsCounter.class.getName())) {
      JSONObject graphStats = jsonSummary.getJSONObject(
          StatsCounter.class.getName());

      String name = StatsCounter.FINAL_STAGE.toString();
      GraphStage stage = GraphStage.values()[graphStats.getIntValue(name)];
      progress.append(stage.getName());
      if (stage.equals(GraphStage.GRAPH_STAGE_SUPERSTEP)
          && graphStats.entrySet().size() > 1) {
        progress.append("\t[");
        addIfExists(graphStats, StatsCounter.TOTAL_SUPERSTEPS.toString(), "step", progress);
        addIfExists(graphStats, StatsCounter.TOTAL_VERTICES.toString(), "vertices", progress);
        addIfExists(graphStats, StatsCounter.TOTAL_EDGES.toString(), "edges", progress);
        addIfExists(graphStats, StatsCounter.TOTAL_HALTED_VERTICES.toString(), "halted", progress);
        addIfExists(graphStats, StatsCounter.TOTAL_SENT_MESSAGES.toString(), "messages", progress);
        addIfExists(graphStats, StatsCounter.TOTAL_WORKERS.toString(), "workers", progress);
        addIfExists(graphStats, StatsCounter.TOTAL_RUNNING_WORKERS.toString(), "running", progress);
        if (jsonSummary.containsKey(MemoryCounter.class.getName())) {
          addIfExists(jsonSummary.getJSONObject(MemoryCounter.class.getName()),
                      MemoryCounter.MAX_USED_MEMORY_WORKER.toString(), "max_mem_workerid",
                      progress);
          addIfExists(jsonSummary.getJSONObject(MemoryCounter.class.getName()),
                      MemoryCounter.MAX_USED_MEMORY.toString(), "max_mem", progress);
        }
        progress.deleteCharAt(progress.length() - 1);
        progress.append("]");
      } else if (stage.equals(GraphStage.GRAPH_STAGE_WAIT_WORKER_UP)
                 && graphStats.entrySet().size() > 1) {
        progress.append("\t");
        addIfExists(graphStats, StatsCounter.TOTAL_RUNNING_WORKERS.toString(), "running", progress);
        progress.deleteCharAt(progress.length() - 1);
      }
    }

    return progress.toString();
  }
}
