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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.Survey;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.RecordComparator;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.io.WritableComparator;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.utils.ReflectionUtils;

/**
 * JobConf 描述了一个ODPS MapReduce 作业的配置.
 *
 * <p>
 * JobConf 是用户向 ODPS 描述一个 M/R 作业如何执行的主要接口。MapReduce 框架会按照 JobConf 描述的信息完成这个作业。
 * </p>
 *
 * <p>
 * JobConf 可以指定 {@link com.aliyun.odps.mapred.Mapper}、Combiner、
 * {@link com.aliyun.odps.mapred.Reducer}等的具体实现。
 * </p>
 *
 * <p>
 * JobConf 可以指定 {@link com.aliyun.odps.mapred.Mapper} 输出的
 * Key/Value的类型和排序、分组用到的列，相关的接口有：
 * <ul>
 * <li>{@link #setMapOutputKeySchema(Column[])}</li>
 * <li>{@link #setMapOutputValueSchema(Column[])}</li>
 * <li>{@link #setOutputKeySortColumns(String[])}</li>
 * <li>{@link #setOutputGroupingColumns(String[])}</li>
 * </ul>
 * </p>
 *
 * <p>
 * JobConf 可以用{@link #addResource(String)}声明本作业用到的 ODPS 资源。
 * </p>
 *
 * <p>
 * JobConf 可以指定一些高级选项，指示 MapReduce 框架该如何执行这个作业，例如：
 * <ul>
 * <li>通过{@link #setSplitSize(long)}影响Mapper的任务数，具体如何影响见 {@link Mapper}；</li>
 * <li>通过{@link #setNumReduceTasks(int)}指定Reducer的任务数</li>
 * </ul>
 * </p>
 *
 * <p>
 * JobConf job = new JobConf();<br/>
 * 构造JobConf对象会自动加载 {@link SessionState} 指定的默认配置文件。
 *
 * <p>
 * 代码示例，摘自WordCount：
 *
 * <pre>
 * JobConf job = new JobConf();
 * job.setMapperClass(TokenizerMapper.class);
 * job.setCombinerClass(SumCombiner.class);
 * job.setReducerClass(SumReducer.class);
 *
 * job.setMapOutputKeySchema(SchemaUtils.fromString(&quot;word:string&quot;));
 * job.setMapOutputValueSchema(SchemaUtils.fromString(&quot;count:bigint&quot;));
 *
 * InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
 * OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
 *
 * RunningJob rj = JobClient.runJob(job);
 *
 * </pre>
 *
 * </blockquote>
 * </p>
 *
 * @see JobClient
 */
public class JobConf extends Configuration {

  private static final Log LOG = LogFactory.getLog(JobConf.class);

  public enum SortOrder {
    ASC, DESC
  }

  /**
   * 构造一个 MapReduce 作业配置.
   */
  public JobConf() {
    super(SessionState.get().getDefaultJob());
  }

  /**
   * 构造一个 MapReduce 作业配置.
   *
   * @param conf
   *     配置管理器
   */
  public JobConf(Configuration conf) {
    super(SessionState.get().getDefaultJob());
    mergeConfiguration(conf);
  }

  /**
   * 构造一个 MapReduce 作业配置.
   *
   * 传入一个Configuration-format XML 配置文件，格式示例：
   *
   * <pre>
   * &lt;configuration&gt;
   *  &lt;property&gt;
   *   &lt;name&gt;com.mycomp.xxx&lt;/name&gt;
   *   &lt;value&gt;xxx&lt;/value&gt;
   *  &lt;/property&gt;
   *  ... ...
   * &lt;/configuration&gt;
   * </pre>
   *
   * @param config
   *     Configuration-format XML 配置文件
   */
  @Survey
  public JobConf(String config) {
    this();
    addResource(config);
  }

  /**
   * 构造一个 MapReduce 作业配置。可选是否载入默认配置。
   *
   * @param loadSessionContext
   *     true如果载入默认配置。
   */
  public JobConf(boolean loadSessionContext) {
    super();
    if (loadSessionContext) {
      mergeConfiguration(SessionState.get().getDefaultJob());
    }
  }

  /**
   * 合并一个配置, 以传入的配置为主。
   *
   * @param conf
   *     需要合并的配置。
   */
  private void mergeConfiguration(Configuration conf) {
    Iterator<Map.Entry<String,String>> iter = conf.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> entry = iter.next();
      this.set(entry.getKey(), entry.getValue());
    }
  }

  /**
   * 声明本作业使用的资源.
   *
   * <p>
   * 只有声明的资源才能在运行 Mapper/Reducer 时通过上下文对象读取。
   *
   * @param resourceNames
   *     资源名称列表，多个资源用逗号分隔
   * @see com.aliyun.odps.mapred.TaskContext
   */
  public void setResources(String resourceNames) {
    set(CONF.RESOURCES, resourceNames);
  }

  /**
   * 获取本作业使用的资源
   *
   * @return 本作业使用的资源列表
   */
  public String[] getResources() {
    return getStrings(CONF.RESOURCES);
  }

  /**
   * 获取 {@link com.aliyun.odps.mapred.Mapper} 输出到
   * {@link com.aliyun.odps.mapred.Reducer} 的 Key 行属性
   *
   * @return {@link com.aliyun.odps.mapred.Mapper} 输出到
   * {@link com.aliyun.odps.mapred.Reducer} 的 Key 行属性
   */
  public Column[] getMapOutputKeySchema() {
    String rs = get(CONF.MAPOUTPUT_KEY_SCHEMA);
    return SchemaUtils.fromString(rs);
  }

  public Column[] getMapOutputKeySchema4Pileline(int nodeId) {
    String rs = get(String.format(CONF.MR2SQL_PIPELINE_OUTPUT_KEY_SCHEMA, nodeId));
    return SchemaUtils.fromString(rs);
  }

  public Column[] getMapOutputValueSchema4Pileline(int nodeId) {
    String rs = get(String.format(CONF.MR2SQL_PIPELINE_OUTPUT_VALUE_SCHEMA, nodeId));
    return SchemaUtils.fromString(rs);
  }

  public String getStreamProcessor(String taskType) {
    return get(String.format(CONF.MR2SQL_STREAMJOB_PROCESSOR, taskType));
  }

  public String getPartitioner(int nodeId) {
    return get(String.format(CONF.MR2SQL_PARTITION_CLASS, nodeId));
  }

  public boolean isStreamJob() {
    return get(CONF.MR2SQL_STREAMJOB_ADDENVIRONMENT) != null;
  }

  public boolean isPipeline() {
    return get(CONF.MR2SQL_PIPELINE_LIST) != null;
  }

  public void setFunctionResources(String resourceNames) {
    set(CONF.MR2SQL_FUNCTION_RESOURCES, resourceNames);
  }

  public String[] getFunctionResources() {
    return getStrings(CONF.MR2SQL_FUNCTION_RESOURCES);
  }

  public void setFunctionCreateText(String text) {
    set(CONF.MR2SQL_FUNCTION_CREATE_TEXT, text);
  }

  public String getFunctionCreateText() {
    return get(CONF.MR2SQL_FUNCTION_CREATE_TEXT);
  }

  public void setFunctionDropText(String text) {
    set(CONF.MR2SQL_FUNCTION_DROP_TEXT, text);
  }

  public String getFunctionDropText() {
    return get(CONF.MR2SQL_FUNCTION_DROP_TEXT);
  }

  /**
   * 设置 {@link com.aliyun.odps.mapred.Mapper} 输出到
   * {@link com.aliyun.odps.mapred.Reducer} 的 Key 行属性
   *
   * @param schema
   *     {@link com.aliyun.odps.mapred.Mapper} 输出到
   *     {@link com.aliyun.odps.mapred.Reducer} 的 Key 行属性
   */
  public void setMapOutputKeySchema(Column[] schema) {
    set(CONF.MAPOUTPUT_KEY_SCHEMA, SchemaUtils.toString(schema));
  }

  /**
   * 获取 {@link com.aliyun.odps.mapred.Mapper} 输出到
   * {@link com.aliyun.odps.mapred.Reducer} 的 Value 行属性
   *
   * @return {@link com.aliyun.odps.mapred.Mapper} 输出到
   * {@link com.aliyun.odps.mapred.Reducer} 的 Value 行属性
   */
  public Column[] getMapOutputValueSchema() {
    String rs = get(CONF.MAPOUTPUT_VALUE_SCHEMA);
    return SchemaUtils.fromString(rs);
  }

  /**
   * 设置 {@link com.aliyun.odps.mapred.Mapper} 输出到
   * {@link com.aliyun.odps.mapred.Reducer} 的 Value 行属性
   *
   * @param schema
   *     {@link com.aliyun.odps.mapred.Mapper} 输出到
   *     {@link com.aliyun.odps.mapred.Reducer} 的 Value 行属性
   */
  public void setMapOutputValueSchema(Column[] schema) {
    set(CONF.MAPOUTPUT_VALUE_SCHEMA, SchemaUtils.toString(schema));
  }

  /**
   * 获取 {@link com.aliyun.odps.mapred.Mapper} 输出到
   * {@link com.aliyun.odps.mapred.Reducer} 的 Key 排序列. 默认是Key的所有列.
   * 排序列是列名组成的有序数组. 如果数组的长度大于1, 输出Key将先按照排序列的第一列排序, 当第一列相等时, 参考第二列, 以此类推.
   * 排序策略是按照对应列的类型的自然序.
   *
   * @return Key的排序列
   * @see #setMapOutputKeySchema(Column[])
   */
  public String[] getOutputKeySortColumns() {
    String joined = get(CONF.OUTPUT_KEY_SORT_COLUMNS, null);
    if (joined == null || joined.isEmpty()) {
      return SchemaUtils.getNames(getMapOutputKeySchema());
    }
    return joined.split(",");
  }

  /**
   * 设置 {@link com.aliyun.odps.mapred.Mapper} 输出到
   * {@link com.aliyun.odps.mapred.Reducer} 的 Key 排序列
   *
   * @param cols
   *     Mapper输出作为排序的列
   */
  public void setOutputKeySortColumns(String[] cols) {
    set(CONF.OUTPUT_KEY_SORT_COLUMNS, StringUtils.join(cols, ","));
  }

  /**
   * 获取 Key 排序列的顺序设定. 默认是Key的所有列升序.
   *
   * @return Key的排序列.
   * @see #setMapOutputKeySchema(Column[])
   */
  public SortOrder[] getOutputKeySortOrder() {
    SortOrder[] order;
    String joined = get(CONF.OUTPUT_KEY_SORT_ORDER, null);
    if (joined == null || joined.isEmpty()) {
      order = new SortOrder[getOutputKeySortColumns().length];
      Arrays.fill(order, SortOrder.ASC);
    } else {
      String[] orders = joined.split(",");
      order = new SortOrder[orders.length];
      for (int i = 0; i < order.length; i++) {
        order[i] = SortOrder.valueOf(orders[i]);
      }
    }
    return order;
  }

  /**
   * 设置 Key 排序列的顺序
   *
   * @param order
   *     排序列的顺序
   */
  public void setOutputKeySortOrder(SortOrder[] order) {
    set(CONF.OUTPUT_KEY_SORT_ORDER, StringUtils.join(order, ","));
  }

  /**
   * 获取指定的 Key 分组比较列，默认为Key的所有列.
   * <p>
   * 关于 Key 分组比较器在 MapReduce 框架中如何被使用，参见 {@link com.aliyun.odps.mapred.Reducer}
   * </p>
   *
   * @return Key 分组比较器
   * @see #setOutputGroupingColumns(String[])
   */
  public String[] getOutputGroupingColumns() {
    String joined = get(CONF.OUTPUT_GROUP_COLUMNS, null);
    if (joined != null) {
      return joined.split(",");
    }
    return SchemaUtils.getNames(getMapOutputKeySchema());
  }

  /**
   * 设置 Key 分组列.
   *
   * <p>
   * 关于 Key 分组列在 MapReduce 框架中如何被使用，参见 {@link com.aliyun.odps.mapred.Reducer}
   * </p>
   *
   * @param cols
   *     Key 分组分组列
   * @see #setMapOutputKeySchema(Column[])
   */
  public void setOutputGroupingColumns(String[] cols) {
    set(CONF.OUTPUT_GROUP_COLUMNS, StringUtils.join(cols, ","));
  }
  
  /**
   * 获取指定的 {@link Mapper} 输出 Key 排序比较器，如果没有指定，默认使用
   * {@link WritableComparator#get(Class)} 返回的比较函数.
   * 
   * @see RecordComparator
   * @see Mapper
   * @return {@link Mapper} 输出 Key 排序比较器.
   * 
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends RecordComparator> getOutputKeyComparatorClass() {
    return getClass(CONF.OUTPUT_KEY_COMPARATOR_CLASS, null, RecordComparator.class);
  }

  /**
   * 设置 {@link Mapper} 输出 Key 排序比较器.
   * 
   * <p>
   * 关于 Key 排序比较器在 MapReduce 总如何被使用，参见 {@link Mapper} 和 {@link Reducer} 中关于
   * Shuffle的解释。
   * </p>
   * 
   * @param theClass
   *          用于 {@link Mapper} 输出 Key 排序的比较器，{@link RecordComparator} 子类
   * @see #setOutputKeyGroupingComparatorClass(Class)
   * 
   */
  @SuppressWarnings("rawtypes")
  public void setOutputKeyComparatorClass(Class<? extends RecordComparator> theClass) {
    setClass(CONF.OUTPUT_KEY_COMPARATOR_CLASS, theClass, RecordComparator.class);
    set(SessionState.MR_EXECUTION_MODE, "lot");
  }

  /**
   * 获取指定的 Key 分组比较器，默认为 {@link #getOutputKeyComparatorClass()}.
   * <p>
   * 关于 Key 分组比较器在 MapReduce 框架中如何被使用，参见 {@link Reducer}
   * </p>
   * 
   * @see #setOutputKeyGroupingComparatorClass(Class)
   * @return Key 分组比较器
   * 
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends RecordComparator> getOutputKeyGroupingComparatorClass() {
    return getClass(CONF.OUTPUT_KEY_GROUPING_COMPARATOR_CLASS, null, RecordComparator.class);
  }

  /**
   * 设置 Key 分组比较器，如果不指定，默认使用 {@link #getOutputKeyComparatorClass()} 作为分组比较器.
   * 
   * <p>
   * 关于 Key 分组比较器在 MapReduce 框架中如何被使用，参见 {@link Reducer}
   * </p>
   * 
   * @param theClass
   *          Key 分组比较器，实现 {@link RecordComparator}接口
   * @see #setOutputKeyComparatorClass(Class)
   * 
   */
  @SuppressWarnings("rawtypes")
  public void setOutputKeyGroupingComparatorClass(Class<? extends RecordComparator> theClass) {
    setClass(CONF.OUTPUT_KEY_GROUPING_COMPARATOR_CLASS, theClass, RecordComparator.class);
  }

  /**
   * 获取作业指定的 {@link com.aliyun.odps.mapred.Mapper}，不指定默认为
   * {@link com.aliyun.odps.mapred.MapperBase}.
   *
   * @return 作业指定的 {@link com.aliyun.odps.mapred.Mapper} 类.
   */
  public Class<? extends Mapper> getMapperClass() {
    return getClass(CONF.MAP_CLASS, MapperBase.class, Mapper.class);
  }

  /**
   * 设置作业的 {@link com.aliyun.odps.mapred.Mapper}.
   *
   * @param theClass
   *     {@link com.aliyun.odps.mapred.Mapper} 子类
   */
  public void setMapperClass(Class<? extends Mapper> theClass) {
    setClass(CONF.MAP_CLASS, theClass, Mapper.class);
  }

  /**
   * 获取作业指定的分区列. 默认是{@link com.aliyun.odps.mapred.Mapper}输出Key的所有列.
   *
   * @return 作业指定的分区列
   */
  public String[] getPartitionColumns() {
    String joined = get(CONF.PARTITION_COLUMNS, null);
    if (joined != null) {
      return joined.split(",");
    }
    return SchemaUtils.getNames(getMapOutputKeySchema());
  }

  /**
   * 设置作业的分区列. 注意：当用户同时指定{@link JobConf#setPartitionerClass(Class)}和{@link
   * JobConf#setPartitionColumns(String[])}时，用户行为未定义
   * 。
   *
   * @param cols
   *     分区名称数组
   * @see com.aliyun.odps.mapred.Mapper
   */
  public void setPartitionColumns(String[] cols) {
    set(CONF.PARTITION_COLUMNS, StringUtils.join(cols, ","));
  }

  /**
   * 获取作业指定的 {@link com.aliyun.odps.mapred.Reducer}，默认就是
   * {@link com.aliyun.odps.mapred.ReducerBase}
   *
   * @return 作业指定的 {@link com.aliyun.odps.mapred.Reducer}
   */
  public Class<? extends Reducer> getReducerClass() {
    return getClass(CONF.REDUCE_CLASS, ReducerBase.class, Reducer.class);
  }

  /**
   * 设置作业的 {@link com.aliyun.odps.mapred.Reducer}.
   *
   * @param theClass
   *     作业的 {@link com.aliyun.odps.mapred.Reducer}
   */
  public void setReducerClass(Class<? extends Reducer> theClass) {
    setClass(CONF.REDUCE_CLASS, theClass, Reducer.class);
  }

  /**
   * 获取作业指定的 combiner
   *
   * @return 作业指定的 combiner
   */
  public Class<? extends Reducer> getCombinerClass() {
    return getClass(CONF.COMBINE_CLASS, null, Reducer.class);
  }

  /**
   * 设置作业的 combiner.
   *
   * @param theClass
   *     作业的 combiner
   */
  public void setCombinerClass(Class<? extends Reducer> theClass) {
    setClass(CONF.COMBINE_CLASS, theClass, Reducer.class);
  }

  /**
   * 获取作业指定的 partitioner.
   *
   * @return 作业指定的 combiner
   */
  public Class<? extends Partitioner> getPartitionerClass() {
    return getClass(CONF.PARTITION_CLASS, null, Partitioner.class);
  }

  /**
   * 设置作业的 partitioner. 注意：当用户同时指定{@link JobConf#setPartitionerClass(Class)}和{@link
   * JobConf#setPartitionColumns(String[])}时，用户行为未定义
   *
   * @param theClass
   *     作业的 {@link com.aliyun.odps.mapred.Partitioner}
   */
  public void setPartitionerClass(Class<? extends Partitioner> theClass) {
    setClass(CONF.PARTITION_CLASS, theClass, Partitioner.class);
  }

  /**
   * 获取指定的切分大小，单位 MB，默认 256.
   *
   * <p>
   * 此参数会影响到 {@link com.aliyun.odps.mapred.Mapper} 任务数，具体影响见
   * {@link com.aliyun.odps.mapred.Mapper} 关于输入切分的说明。
   * </p>
   *
   * @return 指定的切分大小，单位 MB
   */
  public long getSplitSize() {
    return getLong(CONF.MAP_SPLIT_SIZE, 256);
  }

  /**
   * 设置切分大小，单位 MB，默认 256.
   *
   * <p>
   * 此参数会影响到 {@link com.aliyun.odps.mapred.Mapper} 任务数，具体影响见
   * {@link com.aliyun.odps.mapred.Mapper} 关于输入切分的说明。
   * </p>
   *
   * @param size
   *     切分大小，单位 MB
   */
  public void setSplitSize(long size) {
    setLong(CONF.MAP_SPLIT_SIZE, size);
  }

  /**
   * 获取 {@link com.aliyun.odps.mapred.Mapper} 任务数. 仅当空输入（未设定input）时生效.
   *
   * @return {@link com.aliyun.odps.mapred.Mapper} 任务数
   */
  public int getNumMapTasks() {
    return getInt(CONF.MAP_TASKS, 1);
  }

  /**
   * 设置 {@link com.aliyun.odps.mapred.Mapper} 任务数. 仅当空输入（未设定input）时生效.
   *
   * @param n
   *     {@link com.aliyun.odps.mapred.Mapper} 任务数
   */
  public void setNumMapTasks(int n) {
    setInt(CONF.MAP_TASKS, n);
    setBoolean(CONF.MR2SQL_DYNAMIC_PARALLELISM, false);
  }

  /**
   * 获取 {@link com.aliyun.odps.mapred.Reducer} 任务数.
   *
   * @return {@link com.aliyun.odps.mapred.Reducer} 任务数
   */
  public int getNumReduceTasks() {
    return getInt(CONF.REDUCE_TASKS, 1);
  }

  /**
   * 设置 {@link com.aliyun.odps.mapred.Reducer} 任务数，默认为
   * {@link com.aliyun.odps.mapred.Mapper} 任务数的 1/4.
   *
   * <p>
   * 如果不想起 {@link com.aliyun.odps.mapred.Reducer}，需要设置为0：setNumReduceTasks(0);
   * </p>
   *
   * @param n
   *     {@link com.aliyun.odps.mapred.Reducer} 任务数
   */
  public void setNumReduceTasks(int n) {
    setInt(CONF.REDUCE_TASKS, n);
    setInt(CONF.MR2SQL_REDUCE_INSTACNES, n);
    setBoolean(CONF.MR2SQL_DYNAMIC_PARALLELISM, false);
  }

  /**
   * 获取 {@link com.aliyun.odps.mapred.Mapper} 任务的内存资源，单位：MB，默认值 2048.
   *
   * @return {@link com.aliyun.odps.mapred.Mapper} 任务的内存资源
   */
  public int getMemoryForMapTask() {
    return getInt(CONF.MAP_MEMORY, 2048);
  }

  /**
   * 设置 {@link com.aliyun.odps.mapred.Mapper} 任务的内存资源，单位：MB，默认值 2048.
   *
   * @param mem
   *     {@link com.aliyun.odps.mapred.Mapper} 任务的内存资源
   */
  public void setMemoryForMapTask(int mem) {
    setInt(CONF.MAP_MEMORY, mem);
  }

  /**
   * 获取 {@link com.aliyun.odps.mapred.Reducer} 任务的内存资源，单位：MB，默认值 2048.
   *
   * @return {@link com.aliyun.odps.mapred.Reducer} 任务的内存资源
   */
  public int getMemoryForReduceTask() {
    return getInt(CONF.REDUCE_MEMORY, 2048);
  }

  /**
   * 设置 {@link com.aliyun.odps.mapred.Reducer} 任务的内存资源，单位：MB，默认值 2048.
   *
   * @param mem
   *     {@link com.aliyun.odps.mapred.Reducer} 任务的内存资源
   */
  public void setMemoryForReduceTask(int mem) {
    setInt(CONF.REDUCE_MEMORY, mem);
  }

  /**
   * 获取 Map 端 JVM 虚拟机的内存资源，单位：MB，默认值 1024.
   *
   * @return JVM虚拟机的内存资源
   */
  public int getMemoryForMapperJVM() {
    return getInt(CONF.MAP_JVM_MEMORY, 1024);
  }

  /**
   * 设置 Map 端 JVM 虚拟机的内存资源，单位：MB，默认值 1024.
   *
   * @param mem
   *     JVM虚拟机的内存资源
   */
  public void setMemoryForMapperJVM(int mem) {
    setInt(CONF.MAP_JVM_MEMORY, mem);
  }

  /**
   * 获取 Reduce 端 JVM 虚拟机的内存资源，单位：MB，默认值 1024.
   *
   * @return JVM虚拟机的内存资源
   */
  public int getMemoryForReducerJVM() {
    return getInt(CONF.REDUCE_JVM_MEMORY, 1024);
  }

  /**
   * 设置 Reduce 端 JVM 虚拟机的内存资源，单位：MB，默认值 1024.
   *
   * @param mem
   *     JVM虚拟机的内存资源
   */
  public void setMemoryForReducerJVM(int mem) {
    setInt(CONF.REDUCE_JVM_MEMORY, mem);
  }

  /**
   * 获取 JVM虚拟机的内存资源，单位：MB，默认值 1024.
   *
   * @return JVM虚拟机的内存资源
   */
  public int getMemoryForJVM() {
    return getInt(CONF.JVM_MEMORY, 1024);
  }

  /**
   * 设置 JVM虚拟机的内存资源，单位：MB，默认值 1024.
   *
   * @param mem
   *     JVM虚拟机的内存资源
   */
  public void setMemoryForJVM(int mem) {
    setInt(CONF.JVM_MEMORY, mem);
  }

  /**
   * 获取Combiner的缓存大小（记录条数），默认为1024条。
   *
   * @return Combiner的缓存大小
   */
  public int getCombinerCacheItems() {
    return getInt(CONF.COMBINER_CACHE_ITEMS, 1024);
  }

  /**
   * 设置Combiner的缓存大小（记录条数）
   */
  public void setCombinerCacheItems(int size) {
    setInt(CONF.COMBINER_CACHE_ITEMS, size);
  }

  /**
   * 获取Combiner的缓存spill阈值，默认为0.5, 当combine后记录数超出该阈值所定的比例，
   * 则进行spill操作, 仅在CombinerOptimizeEnable为true的时候才生效。
   *
   * @return Combiner的缓存spill阈值
   */
  public float getCombinerCacheSpillPercent() {
    return getFloat(CONF.COMBINER_CACHE_SPILL_PERCENT, (float)0.5);
  }

  /**
   * 设置Combiner的缓存的spill阈值
   */
  public void setCombinerCacheSpillPercent(float percent) {
    setFloat(CONF.COMBINER_CACHE_SPILL_PERCENT, percent);
  }

  /**
   * 获取是否进行Combiner优化，默认为false
   *
   * @return true or false
   */
  public boolean getCombinerOptimizeEnable() {
    return getBoolean(CONF.COMBINER_OPTIMIZE_ENABLE, false);
  }

  /**
   * 设置是否对Combiner进行优化
   */
  public void setCombinerOptimizeEnable(boolean isCombineOpt) {
    setBoolean(CONF.COMBINER_OPTIMIZE_ENABLE, isCombineOpt);
  }

  /**
   * 获取子任务超时时间（秒）设定，默认600秒。
   *
   * @return 超时时间（秒）
   */
  public int getFunctionTimeout() {
    return getInt(CONF.FUNCTION_TIMEOUT, 600);
  }

  /**
   * 设置子任务超时时间（秒）。
   *
   * @param timeout
   *     超时时间（秒）。合法的设定区间为[1, 3600]。
   */
  public void setFunctionTimeout(int timeout) {
    setInt(CONF.FUNCTION_TIMEOUT, timeout);
  }

  /**
   * 获取Instance优先级。
   *
   * @return 优先级
   */
  public int getInstancePriority() {
    return getInt(CONF.INSTANCE_PRIORITY, 9);
  }

  /**
   * 获取是否覆盖输出表
   *
   * @return 是否覆盖输出表
   *
   */
  public boolean getOutputOverwrite() {
    return getBoolean(CONF.OUTPUT_OVERWRITE, true);
  }

  /**
   * 设置对输出表是否进行覆盖。
   *
   * @param isOverwrite
   *     是否覆盖输出表
   */
  public void setOutputOverwrite(boolean isOverwrite) {
    setBoolean(CONF.OUTPUT_OVERWRITE, isOverwrite);
  }

  /**
   * 获取是否在中间节点输出记录
   *
   * @return 是否中间节点输出记录
   *
   */
  public boolean getInnerOutputEnable() {
    return getBoolean(CONF.INNER_OUTPUT_ENABLE, false);
  }

  /**
   * 设置是否在中间节点输出记录。
   *
   * @param isInnerOutput
   *     是否中间节点输出
   */
  public void setInnerOutputEnable(boolean isInnerOutput) {
    setBoolean(CONF.INNER_OUTPUT_ENABLE, isInnerOutput);
  }

  /**
   * 设置Instance优先级。优先级的取值去见为[0, 9]的整型值，数字越大，优先级越低。
   *
   * (注：公共云环境此参数无效)
   *
   * @param priority
   *     优先级
   */
  public void setInstancePriority(int priority) {
    setInt(CONF.INSTANCE_PRIORITY, priority);
  }

  /**
   * 获取输出行属性.
   *
   * @return 行属性
   */
  @Deprecated
  public Column[] getOutputSchema() {
    try {
      onDeprecated(JobConf.class.getMethod("getOutputSchema"));
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 多路输出时获取指定label表的输出行属性
   *
   * @param label
   *     输出标签
   * @return 输出行属性
   */
  @Deprecated
  public Column[] getOutputSchema(String label) {
    try {
      onDeprecated(JobConf.class.getMethod("getOutputSchema", String.class));
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 设置指定label的输出行属性
   *
   * @param schema
   *     输出行属性
   * @param label
   *     输出标签
   */
  @Deprecated
  public void setOutputSchema(Column[] schema, String label) {
    try {
      onDeprecated(JobConf.class.getMethod("setOutputSchema", Column[].class,
                                           String.class));
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
  }

  private void onDeprecated(Method method) {
    set("odps.deprecated." + method.getDeclaringClass().getCanonicalName() + "." + method.getName(),
        "true");
    LOG.warn("Calling deprecated method:" + method);
  }
}
