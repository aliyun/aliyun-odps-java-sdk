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

package com.aliyun.odps.graph.counters;

public enum StatsCounter {
  TOTAL_SUPERSTEPS, //
  TOTAL_VERTICES, //
  TOTAL_HALTED_VERTICES, //
  TOTAL_EDGES, //
  TOTAL_SENT_MESSAGES, //
  TOTAL_WORKERS, //
  LAST_CHECKPOINTED_SUPERSTEP, //
  FINAL_STAGE, //
  TOTAL_RUNNING_WORKERS, //
  MAX_WORKER_VERTICES, //
  MAX_VERTICES_WORKER, //
  AVG_WORKER_VERTICES, //
  MIN_WORKER_VERTICES, //
  MIN_VERTICES_WORKER,
  TOTAL_FAILOVER_TIMES
}
