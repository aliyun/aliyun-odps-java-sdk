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

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.utils.StringUtils;

/**
 * JobConf 描述了一个ODPS Graph 作业的配置.
 *
 * <p>
 * JobConf 是描述一个 ODPS Graph 作业如何执行的主要接口，{@link GraphJob} 继承了 JobConf，更多的说明请查看
 * {@link GraphJob}。
 *
 * @see GraphJob
 */
public class JobConf extends Configuration {

  /**
   * 作业阶段，包括定义阶段和运行阶段.
   */
  public static enum JobState {
    DEFINE, RUNNING
  }

  ;

  protected JobState state = JobState.DEFINE;

  static {
    Configuration.addDefaultResource("odps-graph.xml");
  }

  /**
   * 构造一个 ODPS Graph 作业配置.
   */
  public JobConf() {
    initResources();
  }

  /**
   * 构造一个 ODPS Graph 作业配置.
   *
   * @param conf
   *     配置管理器
   */
  public JobConf(Configuration conf) {
    super(conf);
    if (!(conf instanceof JobConf)) {
      initResources();
    }
  }

  /**
   * 构造一个 ODPS Graph 作业配置.
   *
   * @param conf
   *     配置管理器
   * @param js
   *     作业初始状态，定义或运行状态
   */
  @Deprecated
  public JobConf(Configuration conf, JobState js) {
    this(conf);
    this.state = js;
  }

  /**
   * 构造一个 ODPS Graph 作业配置.
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
  @Deprecated
  public JobConf(String config) {
    super();
    addResource(config);
    initResources();
  }

  /**
   * 构造一个 ODPS Graph 作业配置，可以指示是否加载 CLASSPATH 路径上的 odps-graph.xml 配置文件.
   *
   * @param loadDefaults
   *     指示是否加载 CLASSPATH 路径上的 odps-graph.xml 配置文件
   */
  @Deprecated
  public JobConf(boolean loadDefaults) {
    super(loadDefaults);
    if (loadDefaults) {
      initResources();
    }
  }

  protected void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("ODPS-0730001: Job in state " + this.state
                                      + " instead of " + state);
    }
  }

  @Override
  protected void checkState(String name) {
    if (state == JobState.RUNNING) {
      String graph = "odps.graph";
      if (name.toLowerCase().startsWith(graph)) {
        throw new IllegalStateException("ODPS-0730001: Cann't set configuration " + name
                                        + " started with odps.graph when job in state "
                                        + this.state);
      }
    }
  }

  /**
   * 声明本作业使用的资源.
   *
   * <p>
   * 只有声明的资源才能在运行时通过上下文对象读取。
   *
   * @param resourceNames
   *     资源名称列表，多个资源用逗号分隔
   * @see WorkerContext
   * @see com.aliyun.odps.cache.DistributedCache
   */
  public void addCacheResources(String resourceNames) {
    appendProperties(GRAPH_CONF.CACHE_RESOURCES, resourceNames);
  }

  /**
   * 声明本作业使用的 jar 包资源.
   * <p>
   * 只有声明的 jar 包资源才能在运行时加载到CLASSPATH
   *
   * @param resourceNames
   *     资源名称列表，多个资源用逗号分隔
   */
  public void addCacheResourcesToClassPath(String resourceNames) {
    appendProperties(GRAPH_CONF.CLASSPATH_RESOURCES, resourceNames);
    appendProperties(GRAPH_CONF.CACHE_RESOURCES, resourceNames);
  }

  /**
   * 增加作业输入.
   *
   * <p>
   * 作业运行过程中，框架会读取输入表的数据为一条条 {@link Record}，传给 {@link GraphLoader} 进行图的加载。
   * </p>
   * <p>
   * <b>示例：</b>
   *
   * <pre>
   * GraphJob job = new GraphJob();
   * job.addInput(TableInfo.builder().tableName(&quot;tbl1&quot;).build());
   * job.addInput(TableInfo.builder().tableName(&quot;tbl2&quot;).partSpec(&quot;ds=1&quot;).build());
   * job.addInput(TableInfo.builder().tableName(&quot;tbl3&quot;).partSpec(&quot;ds=1/pt=2&quot;).build());
   * job.addInput(TableInfo.builder().tableName(&quot;tbl3&quot;).cols(new String[]
   * {&quot;col2&quot;, &quot;col0&quot;}).build());
   * </pre>
   *
   * </p>
   * <p>
   * <b>限制：</b>
   * <ul>
   * <li>输入表或分区要求已经存在，且有读权限
   * <li>调用一次 <i>addInput</i> 视为一路输入，ODPS Graph
   * 单个作业的输入路数不能超过1024，且表的数量不能超过64，注意这里并非限制最多读 1024 分区
   * <li>不支持通配符表名或分区范围查询的方式指定作业输入
   * </ul>
   * </p>
   *
   * @param tbl
   *     输入表信息
   * @throws IOException
   * @see GraphLoader
   */
  public void addInput(TableInfo tbl) throws IOException {
    addInput(tbl, tbl.getCols());
  }


  /**
   * 增加作业输入，支持指定读取部分列.
   *
   * <p>
   * 作业运行过程中，框架会读取输入表的数据为一条条 {@link Record}，传给 {@link GraphLoader} 进行图的加载。
   * </p>
   * <p>
   * 本方法支持只读取输入表的部分列，一方面，ODPS
   * 的表使用列存储，读取部分列只扫描部分列的数据，可以改善性能；另一方面，只有读取部分列的权限，这时也需要指定列。当指定读取的列时，
   * {@link GraphLoader} 处理的 {@link Record} 只包含指定的列，且顺序一致。
   * </p>
   *
   * <p>
   * <b>示例：</b>
   *
   * <pre>
   * GraphJob job = new GraphJob();
   *
   * job.addInput(TableInfo.builder().tableName(&quot;tbl1&quot;).build(), new String[] {
   * &quot;col2&quot;, &quot;col0&quot; });
   * job.addInput(TableInfo.builder().tableName(&quot;tbl1&quot;).partSpec(&quot;ds=1/pt=2&quot;).build(),
   * new String[] { &quot;col2&quot;,
   *     &quot;col0&quot; });
   * </pre>
   *
   * </p>
   * <p>
   * <b>限制：</b>
   * <ul>
   * <li>输入表或分区要求已经存在，且对指定列具有读权限
   * <li>调用一次 <i>addInput</i> 视为一路输入，ODPS Graph
   * 单个作业的输入路数不能超过1024，且表的数量不能超过64，注意这里并非限制最多读 1024 分区
   * <li>不支持通配符表名或分区范围查询的方式指定作业输入
   * </ul>
   * </p>
   *
   * @param tbl
   *     输入表信息
   * @param cols
   *     指定读取的列
   * @see GraphLoader
   */
  public void addInput(TableInfo tbl, String[] cols) throws IOException {

    tbl.validate();
    
    String inputDesc = get(GRAPH_CONF.INPUT_DESC, "[]");
    JSONArray array = JSON.parseArray(inputDesc);
    array.add(toJson(tbl, cols));
    set(GRAPH_CONF.INPUT_DESC, JSON.toJSONString(array));
  }


  /**
   * 增加给定 label 的作业输出，覆盖原数据.
   *
   * @param tbl
   *     输出表信息
   * @throws IOException
   */
  public void addOutput(TableInfo tbl) throws IOException {
    addOutput(tbl, true);
  }


  /**
   * 增加给定 label 的作业输出，可以设置是否覆盖原数据.
   *
   * <p>
   * ODPS Graph 作业支持多路输出（其中一路为默认输出，无需指定 label），支持 INSERT OVERWRITE （覆盖）和 INSERT
   * INTO （追加）语义。在运行时可以通过
   * {@link WorkerContext#write(com.aliyun.odps.io.Writable...)} 或
   * {@link WorkerContext#write(String, com.aliyun.odps.io.Writabl...)}
   * 方法写记录到此输出。
   * </p>
   *
   * <p>
   * <b>示例：</b>
   *
   * <pre>
   * {@code
   *
   * // 定义输出
   * GraphJob job = new GraphJob();
   * job.addOutput(TableInfo.builder().tableName("multi_out_t1").build());
   * job.addOutput(TableInfo.builder().tableName("multi_out_t2").partSpec("a=1/b=1").label("out1").build());
   * job.addOutput(TableInfo.builder().tableName("multi_out_t2").partSpec("a=2/b=2").label("out2").build());
   * job.addOutput(TableInfo.builder().projectName("proj").tableName("multi_out_t3").label("out3").build,
   * true);
   *
   * //输出记录
   *   public static class VertexClass extends Vertex<...> {
   *     ...
   *     public void cleanup(WorkerContext<...> context) throws IOException {
   *       context.write(getId(), getValue());
   *       context.write("out1", ...);
   *       context.write("out2", ...);
   *       context.write("out3", ...);
   *     }
   *   }
   * </pre>
   *
   * </p>
   * <p>
   * <b>注意：</b>
   * <ul>
   * <li>输出表要求已经存在（分区可以不存在，不存在时自动创建分区），且有写权限；
   * <li>输出表为分区表时需要给到最末一级分区，不支持输出到动态分区，例如表 partitioned by (a string, b
   * string)，则支持输出到分区 "a=1/b=1"，不支持输出到分区 "a=1"；
   * <li>调用一次 <i>addOutput</i> 视为一路输出，ODPS Graph 单个作业的输出路数不能超过 256；
   * <li>多路输出中指定的 label 不能为 null 或者为空字符串，长度不能超过256，只能包括A-Z，a-z，0-9，_，#，.，-等；
   * <li>INSERT INTO（追加）会使得作业无法重入，重试时可能导致多数据，慎用！
   * </ul>
   * </p>
   *
   * @param tbl
   *     输出表信息
   * @param overwrite
   *     是否覆盖原数据
   * @throws IOException
   */
  public void addOutput(TableInfo tbl, boolean overwrite) throws IOException {
    if (!tbl.getLabel().equals(TableInfo.DEFAULT_LABEL)) {
      processOutput(tbl, tbl.getLabel(), overwrite);
    } else {
      processOutput(tbl, null, overwrite);
    }
  }


  /**
   * 设置作业优先级，范围：[0, 9]，默认9.
   * 0为最高，9为最低.
   *
   * @param priority
   *     作业优先级，范围：[0, 9]，默认0. (注：公共云环境此参数无效)
   */
  public void setJobPriority(int priority) {
    setInt(GRAPH_CONF.JOB_PRIORITY, priority);
  }

  /**
   * 获取作业优先级，范围：[0, 9]，默认0.
   * 0为最高，9为最低.
   *
   * @return 作业优先级
   */
  public int getJobPriority() {
    return getInt(GRAPH_CONF.JOB_PRIORITY, 9);
  }

  /**
   * 获取指定的切分大小，单位 MB，默认 64.
   *
   * @return 指定的切分大小，单位 MB
   */
  public long getSplitSize() {
    return getLong(GRAPH_CONF.SPLIT_SIZE, 64);
  }

  /**
   * 设置输入的切分大小，单位 MB，默认 64.
   *
   * @param size
   *     切分大小
   */
  public void setSplitSize(long size) {
    setLong(GRAPH_CONF.SPLIT_SIZE, size);
  }

  /**
   * 获取 Worker CPU，默认 200，表示两个 CPU 核.
   *
   * @return Worker CPU
   */
  public int getWorkerCPU() {
    return getInt(GRAPH_CONF.WORKER_CPU, 200);
  }

  /**
   * 设置 Worker CPU，默认 200，表示两个 CPU 核.
   *
   * @param cpu
   *     CPU 资源，100 为一个 CPU 核
   */
  public void setWorkerCPU(int cpu) {
    setInt(GRAPH_CONF.WORKER_CPU, cpu);
  }

  /**
   * 设置 Worker 内存，单位MB，默认 4096.
   *
   * @param mem
   *     内存资源，单位MB
   */
  public void setWorkerMemory(int mem) {
    setInt(GRAPH_CONF.WORKER_MEMORY, mem);
  }

  /**
   * 获取 Worker 内存设置，单位MB，默认 4096.
   *
   * @return Worker 内存
   */
  public int getWorkerMemory() {
    return getInt(GRAPH_CONF.WORKER_MEMORY, 4096);
  }

  /**
   * 获取任务的日志级别，包括：ALL，DEBUG，INFO，WARN，ERROR，FATAL，OFF 默认为INFO
   *
   * @param level
   *     日志级别
   */
  public void setLogLevel(String level) {
    if (level == null) {
      throw new RuntimeException("Job log level can't be null");
    }
    this.set(GRAPH_CONF.LOG_LEVEL, level);
  }

  /**
   * 设置图加载实现类，继承 {@link GraphLoader}.
   *
   * <p>
   * Graph Worker 载入图时会使用用户自定义的 {@link GraphLoader} 解析表记录为图的点或边。
   * </p>
   *
   * @param cls
   *     GraphLoader 的实现类
   */
  @SuppressWarnings("rawtypes")
  public void setGraphLoaderClass(Class<? extends GraphLoader> cls) {
    setClass(GRAPH_CONF.GRAPH_LOADER_CLASS, cls, GraphLoader.class);
  }

  /**
   * 获取指定的 {@link GraphLoader} 实现.
   *
   * @return GraphLoader 的实现类
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends GraphLoader> getGraphLoaderClass() {
    return getClass(GRAPH_CONF.GRAPH_LOADER_CLASS, null, GraphLoader.class);
  }

  /**
   * 设置 Vertex 的实现.
   *
   * @param cls
   *     Vertex 的实现类
   */
  @SuppressWarnings("rawtypes")
  public void setVertexClass(Class<? extends Vertex> cls) {
    setClass(GRAPH_CONF.VERTEX_CLASS, cls, Vertex.class);
  }

  /**
   * 获取指定的 Vertex 的实现类.
   *
   * @return Vertex 的实现类
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends Vertex> getVertexClass() {
    return getClass(GRAPH_CONF.VERTEX_CLASS, null, Vertex.class);
  }

  /**
   * 设置 WorkerComputer 的实现.
   *
   * @param cls
   *     WorkerComputer 的实现类
   */
  @SuppressWarnings("rawtypes")
  public void setWorkerComputerClass(Class<? extends WorkerComputer> cls) {
    setClass(GRAPH_CONF.WORKER_COMPUTER_CLASS, cls, WorkerComputer.class);
  }

  /**
   * 获取指定的 WorkerComputer 的实现类.
   *
   * @return WorkerComputer 的实现类
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends WorkerComputer> getWorkerComputerClass() {
    return getClass(GRAPH_CONF.WORKER_COMPUTER_CLASS, WorkerComputer.class,
                    WorkerComputer.class);
  }

  /**
   * 设置 Combiner 的实现.
   *
   * @param cls
   *     Combiner 的实现类
   */
  @SuppressWarnings("rawtypes")
  public void setCombinerClass(Class<? extends Combiner> cls) {
    setClass(GRAPH_CONF.COMBINER_CLASS, cls, Combiner.class);
  }

  /**
   * 获取指定的 Combiner 的实现类.
   *
   * @return Combiner 的实现类
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends Combiner> getCombinerClass() {
    return getClass(GRAPH_CONF.COMBINER_CLASS, null, Combiner.class);
  }

  /**
   * 设置 Partitioner 的实现.
   *
   * @param cls
   *     Partitioner 的实现类
   */
  @SuppressWarnings("rawtypes")
  public void setPartitionerClass(Class<? extends Partitioner> cls) {
    setClass(GRAPH_CONF.PARTITIONER_CLASS, cls, Partitioner.class);
  }

  /**
   * 获取指定的 Partitioner 的实现类.
   *
   * @return Partitioner 的实现类
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends Partitioner> getPartitionerClass() {
    return getClass(GRAPH_CONF.PARTITIONER_CLASS, null, Partitioner.class);
  }

  /**
   * 设置 Aggregator 的实现类.
   *
   * <p>
   * 如果需要设置多个 Aggregator，请使用 {@link #setAggregatorClass(Class...)} 接口。
   * </p>
   *
   * @param cls
   *     Aggregator 的实现类
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void setAggregatorClass(Class<? extends Aggregator> cls) {
    setAggregatorClass(new Class[]{cls});
  }

  /**
   * 设置多个 Aggregator 的实现类.
   *
   * @param cls
   *     多个 Aggregator 的实现类
   */
  @SuppressWarnings("rawtypes")
  public void setAggregatorClass(Class<? extends Aggregator>... cls) {
    String classNames = "";
    for (Class<? extends Aggregator> cl : cls) {
      classNames += cl.getName() + ";";
    }
    set(GRAPH_CONF.AGGREGATOR_CLASSES, classNames);
  }

  /**
   * 设置 AggregatorOwnerPartitioner 的实现.
   *
   * @param cls
   *     AggregatorOwnerPartitioner 的实现类
   */
  public void setAggregatorOwnerPartitionerClass(Class<? extends AggregatorOwnerPartitioner> cls) {
    setClass(GRAPH_CONF.AGGREGATOR_OWNER_PARTITIONER_CLASS, cls, AggregatorOwnerPartitioner.class);
  }

  /**
   * 获取指定的 AggregatorOwnerPartitioner 的实现类.
   *
   * @return AggregatorOwnerPartitioner 的实现类
   */
  public Class<? extends AggregatorOwnerPartitioner> getAggregatorOwnerPartitionerClass() {
    return getClass(GRAPH_CONF.AGGREGATOR_OWNER_PARTITIONER_CLASS, null,
                    AggregatorOwnerPartitioner.class);
  }

  /**
   * 设置是否开启树状Aggregator功能.
   *
   * @param useTreeAggregator
   *     是否开启树状Aggregator功能
   */
  public void setUseTreeAggregator(boolean useTreeAggregator) {
    setBoolean(GRAPH_CONF.USE_TREE_AGGREGATOR, useTreeAggregator);
  }

  /**
   * 获取是否开启树状Aggregator功能.
   *
   * @return 是否开启树状Aggregator功能
   */
  public boolean getUseTreeAggregator() {
    return getBoolean(GRAPH_CONF.USE_TREE_AGGREGATOR, false);
  }

  /**
   * 设置树状Aggregator的深度.
   *
   * @param depth
   *     树状Aggregator的深度
   */
  public void setAggregatorTreeDepth(int depth) {
    setInt(GRAPH_CONF.AGGREGATOR_TREE_DEPTH, depth);
  }

  /**
   * 获取树状Aggregator的深度
   *
   * @return 树状Aggregator的深度
   */
  public int getAggregatorTreeDepth() {
    return getInt(GRAPH_CONF.AGGREGATOR_TREE_DEPTH, 2);
  }

  /**
   * 设置是否运行时对图进行重新分片，默认 true.
   *
   * <p>
   * Graph 作业运行时，每个 Worker 负责装载部分点集，
   * <ul>
   * <li>如果 RuntimePartitioning 设置为 true，则这部分点集会使用
   * {@link #setPartitionerClas(Class)} 分发到对应的 Worker 上，这时每个 Worker
   * 装载的点集与迭代开始运行时的点集常常不同；
   * <li>如果 RuntimePartitioning 设置为 false，则不再进行分发，每个 Worker
   * 装载的点集与迭代开始运行时的点集相同，这时
   * {@link ComputeContext#sendMessage(com.aliyun.odps.io.WritableComparable,
   * com.aliyun.odps.io.Writable)}
   * 接口将不能使用，因为 Vertex 在哪个 Worker 上并不由 Partitioner 决定，sendMessage 无法知道该消息发给哪个
   * Worker，有些应用如 Kmeans 不需要进行再次分发；
   * </ul>
   * </p>
   *
   * @param runtimePartitioning
   *     是否运行时对图进行重新分片
   */
  public void setRuntimePartitioning(boolean runtimePartitioning) {
    setBoolean(GRAPH_CONF.RUNTIME_PARTIONING, runtimePartitioning);
  }

  /**
   * 判断是否运行时对图进行重新分片，默认 true.
   *
   * @return 是否运行时对图进行分片
   */
  public boolean getRuntimePartitioning() {
    return getBoolean(GRAPH_CONF.RUNTIME_PARTIONING, true);
  }

  /**
   * 设置最大迭代次数，默认 -1，小于或等于 0 时表示最大迭代次数不作为作业终止条件.
   *
   * @param maxIteration
   *     最大迭代次数
   */
  public void setMaxIteration(int maxIteration) {
    setInt(GRAPH_CONF.MAX_ITERATION, maxIteration);
  }

  /**
   * 获取指定的最大迭代次数，默认 -1，小于或等于 0 时表示最大迭代次数不作为作业终止条件.
   *
   * @return 最大迭代次数
   */
  public int getMaxIteration() {
    return getInt(GRAPH_CONF.MAX_ITERATION, -1);
  }

  /**
   * 设置图加载阶段的图拓扑修改及冲突处理实现类，继承 {@link LoadingVertexResolver}.
   *
   * <p>
   * 框架提供一个默认实现 {@link DefaultLoadingVertexResolver}，用于处理图加载过程中发现的图拓扑修改及冲突.
   * </p>
   *
   * @param cls
   *     指定的{@link LoadingVertexResolver}实现类
   */
  @SuppressWarnings("rawtypes")
  public void setLoadingVertexResolver(Class<? extends LoadingVertexResolver> cls) {
    setClass(GRAPH_CONF.LOADING_VERTEX_RESOLVER_CLASS, cls,
             LoadingVertexResolver.class);
  }

  /**
   * 设置图加载阶段的冲突处理实现类，继承 {@link VertexResolver}.
   *
   * <p>
   * 框架提供一个默认实现 {@link DefaultLoadingVertexResolver}，用于处理图加载过程中发现的冲突.
   * </p>
   *
   * @param cls
   *     指定的{@link VertexResolver}实现类
   * @deprecated use {@link #setLoadingVertexResolver(Class)} instead
   */
  @SuppressWarnings("rawtypes")
  @Deprecated
  public void setLoadingVertexResolverClass(Class<? extends VertexResolver> cls) {
    setClass(GRAPH_CONF.LOADING_VERTEX_RESOLVER_CLASS, cls,
             VertexResolver.class);
  }

  /**
   * 获取指定的图加载阶段的图拓扑修改及冲突处理实现类.
   *
   * @return 指定的{@link LoadingVertexResolver}实现类
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends VertexResolver> getLoadingVertexResolver() {
    return getClass(GRAPH_CONF.LOADING_VERTEX_RESOLVER_CLASS, null,
                    LoadingVertexResolver.class);
  }

  /**
   * 获取指定的图加载阶段的冲突处理实现类.
   *
   * @return 指定的{@link VertexResolver}实现类
   * @deprecated use {@link #getLoadingVertexResolver()} instead
   */
  @SuppressWarnings("rawtypes")
  @Deprecated
  public Class<? extends VertexResolver> getLoadingVertexResolverClass() {
    return getClass(GRAPH_CONF.LOADING_VERTEX_RESOLVER_CLASS, null,
                    VertexResolver.class);
  }

  /**
   * 设置图迭代计算阶段的冲突处理实现类，继承 {@link VertexResolver}.
   *
   * <p>
   * 框架提供一个默认实现 {@link DefaultComputingVertexResolver}，用于处理迭代计算中发现的冲突.
   * </p>
   *
   * @param cls
   *     指定的{@link VertexResolver}实现类
   * @deprecated use {@link #setComputingVertexResolver(Class)} instead
   */
  @SuppressWarnings("rawtypes")
  @Deprecated
  public void setComputingVertexResolverClass(
      Class<? extends VertexResolver> cls) {
    setClass(GRAPH_CONF.COMPUTING_VERTEX_RESOLVER_CLASS, cls,
             VertexResolver.class);
  }

  /**
   * 获取指定的图迭代计算阶段的冲突处理实现类.
   *
   * @return 指定的{@link VertexResolver}实现类
   * @deprecated use {@link #getComputingVertexResolver()} instead
   */
  @SuppressWarnings("rawtypes")
  @Deprecated
  public Class<? extends VertexResolver> getComputingVertexResolverClass() {
    return getClass(GRAPH_CONF.COMPUTING_VERTEX_RESOLVER_CLASS, null,
                    VertexResolver.class);
  }

  /**
   * 设置图迭代计算阶段的冲突处理实现类，继承 {@link VertexResolver}.
   *
   * <p>
   * 框架提供一个默认实现 {@link DefaultComputingVertexResolver}，用于处理迭代计算中发现的冲突.
   * </p>
   *
   * @param cls
   *     指定的{@link VertexResolver}实现类
   */
  @SuppressWarnings("rawtypes")
  public void setComputingVertexResolver(
      Class<? extends ComputingVertexResolver> cls) {
    setClass(GRAPH_CONF.COMPUTING_VERTEX_RESOLVER_CLASS, cls,
             VertexResolver.class);
  }

  /**
   * 获取指定的图迭代计算阶段的冲突处理实现类.
   *
   * @return 指定的{@link ComputingVertexResolver}实现类
   */
  @SuppressWarnings("rawtypes")
  public Class<? extends ComputingVertexResolver> getComputingVertexResolver() {
    return getClass(GRAPH_CONF.COMPUTING_VERTEX_RESOLVER_CLASS, null,
                    ComputingVertexResolver.class);
  }

  /**
   * 设置存储checkpoint的频率
   * <p>
   * 默认不设置表示按系统默认的方式做checkpoint，即间隔10分钟做一次checkpoint，如果设置，合法值是大于或等于0的整数，
   * 0表示不进行checkpoint，大于0，表示期望每隔n轮superstep做checkpoint，若n次superstep的时间小于系统checkpoint
   * 时间间隔（10分钟），也仍然等到10分钟后再做checkpoint.
   * </p>
   *
   * @param frequency
   *     checkpoint频率
   */
  public void setCheckpointSuperstepFrequency(int frequency) {
    setInt(GRAPH_CONF.CHECKPOINT_SUPERSTEP_FREQUENCY, frequency);
  }


  /**
   * 设置是否在Resolve与Compute之间同步
   * <p>
   * 当设置了ComputingVertexResolver后
   * Resolve和Compute之间会有统计信息的变更(totalVertxNum, totalEdgeNum等）
   * 需要多一轮sync操作，当确认自己对这些信息不敏感时，可以选择关掉
   * 默认打开sync
   * </p>
   *
   * @param is_sync
   *     是否在Resolve与Compute之间同步
   */
  public void setSyncBetweenResolveCompute(boolean is_sync) {
    setBoolean(GRAPH_CONF.SYNC_BETWEEN_RESOLVE_COMPUTE, is_sync);
  }


  /**
   * 获取是否在Resolve与Compute之间的同步
   *
   * @return 是否在Resolve与Compute之间同步
   */
  public boolean getSyncBetweenResolveCompute() {
    return getBoolean(GRAPH_CONF.SYNC_BETWEEN_RESOLVE_COMPUTE, true);
  }

  /**
   * 设置作业并行执行的节点数。
   * 默认是-1，节点数由{@link #getSplitSize()}和作业输入数据量计算得到。
   *
   * <p>
   * 此接口与 {@link #setSplitSize(long)} 配合使用，可以提高数据的载入速度。
   * 假设 setNumWorkers 为 workerNum, setSplitSize 为 splitSize, 总输入字节数为 inputSize,
   * 则输入被切分后的块数 splitNum = inputSize / splitSize，workerNum 和 splitNum 之间的关系：
   * <ul>
   * <li>若 splitNum == workerNum，每个 worker 负责载入一个 split;
   * <li>若 splitNum > workerNum，每个 worker 负责载入一个或多个 split;
   * <li>若 splitNum < workerNum, 每个 worker 负责载入零个或一个 split;
   * </ul>
   * 因此，应调节 workerNum 和 splitSize，在满足前两种情况时，数据载入比较快。迭代阶段只调节 workerNum 即可。
   * </p>
   *
   * @param n
   *     期望的节点数
   */
  public void setNumWorkers(int n) {
    setInt(GRAPH_CONF.WORKER_NUM, n);
  }

  /**
   * 获取期望的节点数。
   *
   * @return 期望的节点数
   */
  public int getNumWorkers() {
    return getInt(GRAPH_CONF.WORKER_NUM, -1);
  }

  /**
   * 设置垃圾回收后开始使用disk-backed的内存阀值，默认0.5。
   *
   * @param threshold
   *     内存阀值
   */
  public void setMemoryThreshold(float threshold) {
    setFloat(GRAPH_CONF.MEMORY_THRETHOLD, threshold);
  }

  /**
   * 获取使用disk-backed的内存阀值。
   *
   * @return 内存阀值
   */
  public float getMemoryThreshold() {
    return getFloat(GRAPH_CONF.MEMORY_THRETHOLD, 0.5f);
  }

  /**
   * 设置是否开启disk-backed message，如果开启，则在内存到达阀值后尝试使用磁盘存储收到的message，默认关闭。
   *
   * @param useDiskBackedMessage
   *     是否开启disk-backed message
   */
  public void setUseDiskBackedMessage(boolean useDiskBackedMessage) {
    setBoolean(GRAPH_CONF.USE_DISKBACKED_MESSAGE, useDiskBackedMessage);
  }

  /**
   * 获取是否开启disk-backed message
   *
   * @return 是否开启disk-backed message
   */
  public boolean getUseDiskBackedMessage() {
    return getBoolean(GRAPH_CONF.USE_DISKBACKED_MESSAGE, false);
  }

  /**
   * 设置是否开启disk-backed mutation，如果开启，则在内存到达阀值后尝试使用磁盘存储收到的mutation，默认关闭。
   *
   * @param useDiskBackedMutation
   *     是否开启disk-backed mutation
   */
  public void setUseDiskBackedMutation(boolean useDiskBackedMutation) {
    setBoolean(GRAPH_CONF.USE_DISKBACKED_MUTATION, useDiskBackedMutation);
  }

  /**
   * 获取是否开启disk-backed mutation
   *
   * @return 是否开启disk-backed mutation
   */
  public boolean getUseDiskBackedMutation() {
    return getBoolean(GRAPH_CONF.USE_DISKBACKED_MUTATION, false);
  }

  /**
   * 设置是否开启广播消息，如果开启，在调用{@link ComputeContext#sendMessageToNeighbors(Vertex,
   * com.aliyun.odps.io.Writable)}
   * 时，以广播的方式将消息发送到点的邻居，默认关闭。
   *
   * @param broadcastMessageEnable
   *     是否开启广播
   */
  public void setBroadcastMessageEnable(boolean broadcastMessageEnable) {
    setBoolean(GRAPH_CONF.BROADCAST_MESSAGE_ENABLE, broadcastMessageEnable);
  }

  /**
   * 获取是否开启广播。
   *
   * @return 是否开启广播
   */
  public boolean getBroadcastMessageEnable() {
    return getBoolean(GRAPH_CONF.BROADCAST_MESSAGE_ENABLE, false);
  }

  /**
   * 设置为true后，单个点接收到的message和mutation可以超过2G.
   */
  public void setUseMultipleInputOutput(boolean useMultipleInputOutput) {
    setBoolean(GRAPH_CONF.USE_MULTIPLE_INPUT_OUTPUT, useMultipleInputOutput);
  }

  /**
   * 获取单个点接收到的mutation和message是否可以超过2G.
   */
  public boolean getUseMultipleInputOutput() {
    return getBoolean(GRAPH_CONF.USE_MULTIPLE_INPUT_OUTPUT, false);
  }

  private void initResources() {
    String resources = System.getProperty("odps.cache.resources");
    if (resources != null && !resources.trim().isEmpty()) {
      addCacheResources(resources);
    }

    String libjars = System.getProperty("odps.classpath.resources");
    if (libjars != null && !libjars.trim().isEmpty()) {
      addCacheResourcesToClassPath(libjars);
    }
  }

  private static JSONObject toJson(TableInfo tbl, String[] cols) {
    JSONObject obj = new JSONObject(true);
    String projectName = tbl.getProjectName();
    if (projectName == null) {
      projectName = "";
    }
    obj.put("projName", projectName);
    obj.put("tblName", tbl.getTableName());
    JSONArray array = new JSONArray();
    LinkedHashMap<String, String> partSpec = tbl.getPartSpec();
    for (Map.Entry<String, String> entry : partSpec.entrySet()) {
      String key = StringUtils.strip(entry.getKey(), "'\"");
      String value = StringUtils.strip(entry.getValue(), "'\"");
      array.add(key + "=" + value);
    }
    obj.put("partSpec", array);
    obj.put("cols", cols == null ? "" : StringUtils.join(cols, ','));
    return obj;
  }


  private void processOutput(TableInfo tbl, String label, boolean overwrite) {
    String outputDesc = get(GRAPH_CONF.OUTPUT_DESC, "[]");
    JSONArray array = JSON.parseArray(outputDesc);
    array.add(toJson(tbl, label, overwrite));
    set(GRAPH_CONF.OUTPUT_DESC, JSON.toJSONString(array));
  }

  private JSONObject toJson(TableInfo tbl, String label, boolean overwrite) {
    JSONObject obj = new JSONObject(true);
    String projectName = tbl.getProjectName();
    if (projectName == null) {
      projectName = "";
    }
    obj.put("projName", projectName);
    obj.put("tblName", tbl.getTableName());
    JSONArray array = new JSONArray();
    LinkedHashMap<String, String> partSpec = tbl.getPartSpec();
    for (Map.Entry<String, String> entry : partSpec.entrySet()) {
      array.add(entry.getKey() + "=" + entry.getValue());
    }
    obj.put("partSpec", array);
    String lab = (label == null) ? "" : label;
    obj.put("label", lab);
    obj.put("overwrite", overwrite);
    return obj;
  }

  private void appendProperties(String name, String value) {
    String originalValue = this.get(name);
    if (originalValue == null || originalValue.length() == 0) {
      this.set(name, value);
      return;
    }
    // deduplication
    String[] orginalValues = originalValue.split(",");
    String[] appendValues = value.split(",");
    Set<String> set = new HashSet<String>();
    for (int i = 0; i < orginalValues.length; i++) {
      set.add(orginalValues[i].trim());
    }
    for (int i = 0; i < appendValues.length; i++) {
      if (!set.contains(appendValues[i])) {
        originalValue = originalValue + "," + appendValues[i].trim();
      }
    }
    this.set(name, originalValue);
  }

}
