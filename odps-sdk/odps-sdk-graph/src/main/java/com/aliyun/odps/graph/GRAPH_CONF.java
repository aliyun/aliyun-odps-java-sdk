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

package com.aliyun.odps.graph;

/**
 * 定义 ODPS Graph 作业配置中可以允许用户自定义的配置参数.
 */
public class GRAPH_CONF {

  public final static String JOB_PRIORITY = "odps.graph.job.priority";
  public final static String CACHE_RESOURCES = "odps.graph.cache.resources";
  public final static String CLASSPATH_RESOURCES = "odps.graph.classpath.resources";

  public final static String WORKER_CPU = "odps.graph.worker.cpu";
  public final static String WORKER_MEMORY = "odps.graph.worker.memory";
  public final static String INPUT_DESC = "odps.graph.input.desc";
  public final static String OUTPUT_DESC = "odps.graph.output.desc";
  public final static String SPLIT_SIZE = "odps.graph.split.size";
  public final static String LOG_LEVEL = "odps.graph.log.level";
  public final static String WORKER_NUM = "odps.graph.worker.num";

  public static final String VERTEX_CLASS = "odps.graph.vertex.class";
  public static final String WORKER_COMPUTER_CLASS = "odps.graph.worker.computer.class";
  public static final String MASTER_COMPUTER_CLASS = "odps.graph.master.computer.class";
  public static final String COMBINER_CLASS = "odps.graph.combiner.class";
  public static final String PARTITIONER_CLASS = "odps.graph.partitioner.class";
  public static final String AGGREGATOR_CLASSES = "odps.graph.aggregator.classes";
  public static final String
      AGGREGATOR_OWNER_PARTITIONER_CLASS =
      "odps.graph.aggregator.owner.partitioner.class";
  public static final String USE_TREE_AGGREGATOR = "odps.graph.use.tree.aggregator";
  public static final String AGGREGATOR_TREE_DEPTH = "odps.graph.aggregator.tree.depth";
  public static final String GRAPH_LOADER_CLASS = "odps.graph.loader.class";
  public static final String
      LOADING_VERTEX_RESOLVER_CLASS =
      "odps.graph.loading.vertex.resolver.class";
  public static final String
      COMPUTING_VERTEX_RESOLVER_CLASS =
      "odps.graph.computing.vertex.resolver.class";
  public final static String RUNTIME_PARTIONING = "odps.graph.runtime.partitioning";
  public final static String MAX_ITERATION = "odps.graph.max.iteration";
  public final static String COMPUTING_THREADS = "odps.graph.computing.threads";
  public final static String LOADING_THREADS = "odps.graph.loading.threads";
  public final static String INPUT_SLICE_SIZE = "odps.graph.input.slice.size";

  public final static String WORKER_TIMEOUT = "odps.graph.worker.timeout";
  public static final String
      CHECKPOINT_SUPERSTEP_FREQUENCY =
      "odps.graph.checkpoint.superstep.frequency";
  public static final String MAX_ATTEMPTS = "odps.graph.max.attempts";
  public static final String SYNC_BETWEEN_RESOLVE_COMPUTE = "odps.graph.sync.resolve.compute";

  public static final String USE_DISKBACKED_MESSAGE = "odps.graph.use.diskbacked.message";
  public static final String USE_DISKBACKED_MUTATION = "odps.graph.use.diskbacked.mutation";
  public static final String MEMORY_THRETHOLD = "odps.graph.spill.memory.threshold";

  public static final String BROADCAST_MESSAGE_ENABLE = "odps.graph.broadcast.message.enable";
  public static final String
      USE_BYTEARRAY_PARTITION_ENABLE =
      "odps.graph.use.bytearray.partition.enable";
  public static final String USE_MULTIPLE_INPUT_OUTPUT = "odps.graph.use.multiple.input.output";
}
