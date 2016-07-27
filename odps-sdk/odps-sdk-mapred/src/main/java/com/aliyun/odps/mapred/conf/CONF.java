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

package com.aliyun.odps.mapred.conf;

/**
 * 定义 MapReduce 作业配置中可以允许用户自定义的配置参数.
 */
class CONF {

  /**
   * 当前任务添加的资源，运行时可读取.
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#addResources(String)
   */
  public final static String RESOURCES = "odps.mapred.resources";

  /**
   * 任务的{@link com.aliyun.odps.mapred.Mapper}的输出键值对KEY的行属性, 也是
   * {@link com.aliyun.odps.mapred.Reducer}的输入键值对KEY的行属性
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setMapOutputKeySchema(com.aliyun.odps.Column[])
   * @see com.aliyun.odps.mapred.conf.JobConf#getMapOutputKeySchema()
   */
  public final static String MAPOUTPUT_KEY_SCHEMA = "odps.mapred.mapoutput.key.schema";
  /**
   * 任务的{@link com.aliyun.odps.mapred.Mapper}的输出键值对VALUE的行属性, 该行属性也是
   * {@link com.aliyun.odps.mapred.Reducer}的输入键值对VALUE的行属性
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setMapOutputValueSchema(com.aliyun.odps.Column[])
   * @see com.aliyun.odps.mapred.conf.JobConf#getMapOutputValueSchema()
   */
  public final static String MAPOUTPUT_VALUE_SCHEMA = "odps.mapred.mapoutput.value.schema";
  /**
   * KEY排序使用的列, 在{@link com.aliyun.odps.mapred.Mapper}的输出键值对按照
   * {@link #PARTITION_COLUMNS} 划分后，按这里指定的列名列表排序.对指定的单个列,排序的规则遵循自然序.
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setOutputKeySortColumns(String[])
   * @see com.aliyun.odps.mapred.conf.JobConf#getOutputKeySortColumns()
   */
  public final static String OUTPUT_KEY_SORT_COLUMNS = "odps.mapred.output.key.sort.columns";
  /**
   * KEY排序的顺序设定，对
   * {@link com.aliyun.odps.mapred.conf.JobConf#setOutputKeySortColumns(String[])}
   * 设定的每一个排序列，指定排序顺序。可选的顺序为升序
   * {@link com.aliyun.odps.mapred.conf.JobConf.SortOrder#INC}或降序
   * {@link com.aliyun.odps.mapred.conf.JobConf.SortOrder#DESC}。
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setOutputKeySortColumns(String[])
   * @see com.aliyun.odps.mapred.conf.JobConf#getOutputKeySortColumns()
   */
  public final static String OUTPUT_KEY_SORT_ORDER = "odps.mapred.output.key.sort.order";
  /**
   * 任务的KEY聚合列, 在{@link com.aliyun.odps.mapred.Reducer}的输入键值对依照
   * {@link #OUTPUT_KEY_SORT_COLUMNS}排序后, 该聚合器将聚合后的键值对传递给
   * {@link com.aliyun.odps.mapred.Reducer}.
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setOutputGroupingColumns(String[])
   * @see com.aliyun.odps.mapred.conf.JobConf#getOutputGroupingColumns()
   */
  public final static String OUTPUT_GROUP_COLUMNS = "odps.mapred.output.group.columns";
  /**
   * 任务的{@link com.aliyun.odps.mapred.Mapper}类.
   *
   * @see com.aliyun.odps.mapred.Mapper
   * @see com.aliyun.odps.mapred.conf.JobConf#setMapperClass(Class)
   * @see com.aliyun.odps.mapred.conf.JobConf#getMapperClass()
   */
  public final static String MAP_CLASS = "odps.mapred.map.class";
  /**
   * 任务的{@link com.aliyun.odps.mapred.Mapper}的输出键值对的分区类.
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setPartitionColumns(String[])
   * @see com.aliyun.odps.mapred.conf.JobConf#setPartitionColumns(String[])
   */
  public final static String PARTITION_COLUMNS = "odps.mapred.partition.columns";
  /**
   * 任务的partitioner类.
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setPartitionerClass(Class)
   * @see com.aliyun.odps.mapred.conf.JobConf#getPartitionerClass()
   */
  public final static String PARTITION_CLASS = "odps.mapred.partition.class";
  /**
   * 任务的{@link com.aliyun.odps.mapred.Reducer}类.
   *
   * @see com.aliyun.odps.mapred.Reducer
   * @see com.aliyun.odps.mapred.conf.JobConf#setReducerClass(Class)
   * @see com.aliyun.odps.mapred.conf.JobConf#getReducerClass()
   */
  public final static String REDUCE_CLASS = "odps.mapred.reduce.class";
  /**
   * 任务的combiner类.
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setCombinerClass(Class)
   * @see com.aliyun.odps.mapred.conf.JobConf#getCombinerClass()
   */
  public final static String COMBINE_CLASS = "odps.mapred.combine.class";
  /**
   * 任务的{@link com.aliyun.odps.mapred.Mapper}个数.
   *
   * @see com.aliyun.odps.mapred.Mapper
   * @see com.aliyun.odps.mapred.conf.JobConf#setNumMapTasks(int)
   * @see com.aliyun.odps.mapred.conf.JobConf#getNumMapTasks()
   */
  public final static String MAP_TASKS = "odps.stage.mapper.num";
  /**
   * 任务的{@link com.aliyun.odps.mapred.Reducer}个数.
   *
   * @see com.aliyun.odps.mapred.Reducer
   * @see com.aliyun.odps.mapred.conf.JobConf#setNumReduceTasks(int)
   * @see com.aliyun.odps.mapred.conf.JobConf#getNumReduceTasks()
   */
  public final static String REDUCE_TASKS = "odps.stage.reducer.num";

  /**
   * 任务的{@link com.aliyun.odps.mapred.Mapper} 内存资源
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setMemoryForMapTask(int)
   * @see com.aliyun.odps.mapred.conf.JobConf#getMemoryForMapTask()
   */
  public final static String MAP_MEMORY = "odps.stage.mapper.mem";

  /**
   * 任务的{@link com.aliyun.odps.mapred.Reducer} 内存资源
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setMemoryForReduceTask(int)
   * @see com.aliyun.odps.mapred.conf.JobConf#getMemoryForReduceTask()
   */
  public final static String REDUCE_MEMORY = "odps.stage.reducer.mem";

  /**
   * 任务的JVM内存资源
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setMemoryForJVM(int)
   * @see com.aliyun.odps.mapred.conf.JobConf#getMemoryForJVM()
   */
  public final static String JVM_MEMORY = "odps.stage.jvm.mem";

  /**
   * 任务的 Map JVM 内存资源
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setMemoryForMapperJVM(int)
   * @see com.aliyun.odps.mapred.conf.JobConf#getMemoryForMapperJVM()
   */
  public final static String MAP_JVM_MEMORY = "odps.stage.mapper.jvm.mem";

  /**
   * 任务的 Reduce JVM 内存资源
   *
   * @see com.aliyun.odps.mapred.conf.JobConf#setMemoryForReducerJVM(int)
   * @see com.aliyun.odps.mapred.conf.JobConf#getMemoryForReducerJVM()
   */
  public final static String REDUCE_JVM_MEMORY = "odps.stage.reducer.jvm.mem";

  /**
   * {@link com.aliyun.odps.mapred.Mapper}输入的切分大小
   *
   * @see com.aliyun.odps.mapred.Mapper
   * @see com.aliyun.odps.mapred.conf.JobConf#setSplitSize(long)
   * @see com.aliyun.odps.mapred.conf.JobConf#getSplitSize()
   */
  public final static String MAP_SPLIT_SIZE = "odps.stage.mapper.split.size";

  /**
   * 输出表的行属性
   */
  public static final String OUTPUT_SCHEMA = "odps.mapred.output.schema";

  /**
   * 输入表的行属性
   */
  public static final String INPUT_SCHEMA = "odps.mapred.input.schema";

  /**
   * Combiner缓存记录数
   */
  public final static String COMBINER_CACHE_ITEMS = "odps.mapred.combiner.cache.items";

  /**
   * Combiner缓存的Spill阈值
   */
  public final static String COMBINER_CACHE_SPILL_PERCENT = "odps.mapred.combiner.cache.spill.percent";

  /**
   * Combiner优化开关
   */
  public final static String COMBINER_OPTIMIZE_ENABLE = "odps.mapred.combiner.optimize.enable";

  /**
   * 子任务超时时间（秒）
   */
  public final static String FUNCTION_TIMEOUT = "odps.function.timeout";

  /**
   * Instance优先级设定
   */
  public final static String INSTANCE_PRIORITY = "odps.instance.priority";

  /**
   * 设置是否覆盖输出表
   */
  public final static String OUTPUT_OVERWRITE = "odps.mapred.output.overwrite";

  /**
   * 设置是否在中间节点进行输出
   */
  public final static String INNER_OUTPUT_ENABLE = "odps.mapred.inner.output.enable";

  public static final String OUTPUT_KEY_COMPARATOR_CLASS = "odps.stage.mapred.output.key.comparator.class";

  public static final String OUTPUT_KEY_GROUPING_COMPARATOR_CLASS = "odps.mapred.output.key.grouping.comparator.class";
}
