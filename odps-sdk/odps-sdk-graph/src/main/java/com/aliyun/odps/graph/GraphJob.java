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
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.graph.job.JobRunner;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.utils.ReflectionUtils;
import com.aliyun.odps.utils.StringUtils;

/**
 * GraphJob 继承自 {@link JobConf}，用于定义、提交和管理一个 ODPS Graph 作业.
 *
 * <p>
 * ODPS Graph 作业属于一类 <a
 * href="http://en.wikipedia.org/wiki/Bulk_synchronous_parallel" >BSP (Bulk
 * Synchronous Parallel)</a> 程序，通过构造一个有向图，然后迭代对图进行编辑处理完成计算任务，迭代终止条件允许自定义。
 * </p>
 *
 * <p>
 * 一个 ODPS Graph 程序逻辑如下：
 * <ol>
 * <li><b>加载图：</b>
 * <ul>
 * <li>图是一个由点 和边 （{@link Vertex}/{@link Edge}）组成的有向图，点和边包含值；
 * <li>使用 {@link GraphLoader} 将表的记录解析为点及其出边；
 * <li>分布式化：按点 ID 哈希，分配到相应的 Worker；
 * </ul>
 * <li><b>超步（super step）：</b>
 * <ul>
 * <li>一次迭代为一个超步，遍历所有非结束的点，并调用其
 * {@linkplain Vertex#compute(ComputeContext, Iterable) compute} 方法；
 * <li>在用户实现的 {@linkplain Vertex#compute(ComputeContext, Iterable) compute} 方法中：
 * <ul>
 * <li>处理上一次迭代发给当前点的消息；
 * <li>根据需要对图进行编辑：1）修改点/边的取值；2）发送消息给某些点；3）增加/删除点或边；
 * <li>通过 {@link Aggregator} 汇总信息到全局信息；
 * <li>设置当前点状态，结束或非结束状态；
 * <li>迭代进行过程中，框架会将消息以异步的方式发送到对应 Worker 并在下一轮迭代时进行处理，用户无需关心；
 * </ul>
 * </ul>
 * <li><b>迭代终止（满足以下任意一条）：</b>
 * <ul>
 * <li>所有点处于结束状态且没有新消息产生
 * <li>到达最大迭代次数；
 * <li>某个 {@link Aggregator} 的
 * {@linkplain Aggregator#terminate(WorkerContext, com.aliyun.odps.io.Writable)
 * terminate} 返回true；
 * </ul>
 * </ol>
 * </p>
 *
 * <p>
 * GraphJob 提供了两类接口：
 * </p>
 *
 * <p>
 * <b>第一类：用于定义一个 ODPS Graph 作业，这类接口继承自 {@link JobConf}，主要包括：</b>
 * </p>
 *
 * <p>
 * 指定 ODPS Graph 的具体实现类：
 * <ul>
 * <li>{@link #setWorkerComputerClass(Class)}
 * <li>{@link #setGraphLoaderClass(Class)}
 * <li>{@link #setVertexClass(Class)}
 * <li>{@link #setAggregatorClass(Class)}
 * <li>{@link #setAggregatorClass(Class...)}
 * <li>{@link #setPartitionerClass(Class)}
 * <li>{@link #setCombinerClass(Class)}
 * </ul>
 * 其中，{@link GraphLoader} 和 {@link Vertex} 必须提供实现，其他根据需要可选。
 * </p>
 *
 * <p>
 * 指定作业的输入输出：
 * <ul>
 * <li>{@link #addInput(TableInfo)}
 * <li>{@link #addInput(TableInfo, String[])}
 * <li>{@link #addOutput(TableInfo)}
 * <li>{@link #addOutput(TableInfo, boolean)}
 * <li>{@link #addOutput(TableInfo, String)}
 * <li>{@link #addOutput(TableInfo, String, boolean)}
 * </ul>
 * </p>
 *
 * <p>
 * 声明本作业用到的 ODPS 资源：
 * <ul>
 * <li>{@link #addCacheResources(String)}，此接口作用与 jar -resources 声明资源的效果一样
 * <li>{@link #addCacheResourcesToClassPath(String)} 此接口作用与 jar -libjars
 * 声明资源的效果一样
 * </ul>
 * </p>
 *
 * <p>
 * 指定一些高级选项，指示 ODPS Graph 执行框架该如何执行这个作业，例如： ：
 * <ul>
 * <li>{@link #setSplitSize(long)} 设置输入的切分大小（单位 MB，默认值 256），会影响 Worker 数目；
 * <li>{@link #setRuntimePartitioning(boolean)} 指示 Worker 在加载点后是否进行重新分发，默认值是
 * true；
 * <li>{@link #setMaxIteration(int)} 设置最大迭代次数，最大迭代次数是迭代终止的条件之一，默认值是
 * -1，若取值<=0，则表示最大迭代次数不作为迭代终止条件；
 * </ul>
 * </p>
 *
 * <p>
 * <b>第二类：用于提交和管理一个 ODPS Graph 作业，主要包括：</b>
 * <ul>
 * <li>{@link #run()} 提交作业并等待作业结束，作业失败则会抛异常，阻塞（同步）方式；
 * <li>{@link #submit()} 提交作业立即返回，非阻塞（异步）方式；
 * <li>{@link #isComplete()} 查询作业是否结束（成功、失败或被杀），通常在非阻塞方式提交作业时使用；
 * <li>{@link #isSuccessful()} 查询作业是否成功，通常在非阻塞方式提交作业时使用；
 * <li>{@link #getCounters()} 获取作业计数信息；
 * </ul>
 * </p>
 *
 * <p>
 * 代码示例，摘自PageRank：
 *
 * <pre>
 * {@code
 *   public static void main(String[] args) throws IOException { *
 *     GraphJob job = new GraphJob();
 *
 *     job.setGraphLoaderClass(PageRankGraphLoader.class);
 *     job.setVertexClass(PageRankVertex.class);
 *     job.addInput(new TableInfo(args[0]));
 *     job.addOutput(new TableInfo(args[1]));
 *
 *     job.setMaxIteration(30);
 *
 *     job.run();
 *   } }
 * </pre>
 *
 * </blockquote>
 * </p>
 *
 * @see Vertex
 * @see GraphLoader
 * @see Aggregator
 * @see WorkerComputer
 */
public class GraphJob extends JobConf {

  private static final Log LOG = LogFactory.getLog(GraphJob.class);

  private RunningJob rJob = null;

  /**
   * 构造一个 ODPS Graph 作业.
   */
  public GraphJob() {
    super();
  }

  /**
   * 构造一个 ODPS Graph 作业，可以指示是否加载 CLASSPATH 路径上的 odps-graph.xml 配置文件.
   *
   * @param loadDefaults
   *     指示是否加载 CLASSPATH 路径上的 odps-graph.xml 配置文件
   */
  @Deprecated
  public GraphJob(boolean loadDefaults) {
    super(loadDefaults);
  }

  /**
   * 构造一个 ODPS Graph 作业.
   *
   * @param conf
   *     配置管理器
   * @param js
   *     作业初始状态，定义或运行状态
   */
  @Deprecated
  public GraphJob(Configuration conf, JobState js) {
    super(conf, js);
  }

  /**
   * 构造一个 ODPS Graph 作业.
   *
   * @param conf
   *     配置管理器
   */
  public GraphJob(Configuration conf) {
    super(conf);
  }

  /**
   * 构造一个 ODPS Graph 作业.
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
  public GraphJob(String config) {
    super(config);
  }


  /**
   * 查询作业是否结束.
   *
   * @return 作业结束返回true，否则返回false
   * @throws IOException
   */
  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    return rJob.isComplete();
  }

  /**
   * 查询作业实例是否运行成功.
   *
   * @return 作业成功返回true，否则返回false
   * @throws IOException
   */
  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    return rJob.isSuccessful();
  }

  /**
   * Kill 此作业运行实例
   *
   * @throws IOException
   */
  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    rJob.killJob();
  }

  /**
   * 非阻塞（异步）方式提交 ODPS Graph 作业后立即返回.
   *
   * <p>
   * 只有当提交作业发生异常抛{@link IOException}（注意：这与 {@link #run()} 异常行为不同， {@link #run()}
   * 在作业失败时会抛异常）。
   * </p>
   *
   * <p>
   * 使用本方法提交作业，可以轮询作业状态，示例代码：
   *
   * <pre>
   * GraphJob job = new GraphJob();
   * ... //config job
   * job.submit();
   * while (!job.isComplete()) {
   *   Thread.sleep(4000); // do your work or sleep
   * }
   * if (job.isSuccessful()) {
   *   System.out.println(&quot;Job Success!&quot;);
   * } else {
   *   System.err.println(&quot;Job Failed!&quot;);
   * }
   * </pre>
   *
   * </p>
   *
   * @throws IOException
   *     作业提交失败时抛异常
   */
  public void submit() throws IOException {
    ensureState(JobState.DEFINE);

    try {
      parseArgs();
      String runner = "com.aliyun.odps.graph.job.NetworkJobRunner";
      if (SessionState.get().isLocalRun()) {
        runner = "com.aliyun.odps.graph.local.LocalGraphJobRunner";
      }
      JobRunner jobrunner = null;
      try {
        Class<? extends JobRunner> clz = (Class<? extends JobRunner>) Class.forName(runner);
        jobrunner = ReflectionUtils.newInstance(clz, this);
      } catch (ClassNotFoundException e) {
        LOG.fatal("Internal error: currupted installation.", e);
        throw new RuntimeException(e);
      }
      rJob = jobrunner.submit();

    } catch (OdpsException oe) {
      LOG.error(StringUtils.stringifyException(oe));
      throw new IOException(oe.getMessage());
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(e.getMessage());
    }

    state = JobState.RUNNING;
  }

  /**
   * 阻塞（同步）方式提交 ODPS Graph 作业并等待作业结束.
   *
   * <p>
   * 以下情况发生时抛{@link IOException}：
   * <ul>
   * <li>提交作业时异常
   * <li>轮询作业状态异常
   * <li>作业失败，注意：这与{@link #submit()}异常行为不同
   * </ul>
   * </p>
   *
   * <p>
   * 作业主程序（main函数）需要谨慎处理该异常，因为会影响到console的返回值： <br/>
   * 如果不catch异常，作业失败时会抛出异常，console返回值为非0；如果catch异常且不再向外抛出，即使作业失败，console返回值也为0。<br/>
   * </p>
   *
   * 示例代码：
   *
   * <pre>
   * GraphJob job = new GraphJob();
   * ... //config job
   * job.run();
   * </pre>
   *
   * @throws IOException
   *     如果发生提交作业异常、轮询作业状态异常或者作业失败，则抛 IOException 异常
   * @see #submit()
   */
  public void run() throws IOException {
    if (state == JobState.DEFINE) {
      submit();
    }
    rJob.waitForCompletion();
    if (!rJob.isSuccessful()) {
      throw new IOException("Job failed!");
    }
  }

  /**
   * 获取作业运行实例的 Counters 信息，ODPS Graph 运行框架会汇总所有 Worker 设置的 Counters.
   *
   * @return 作业运行实例的 Counters 信息
   * @throws IOException
   */
  public Counters getCounters() throws IOException {
    return rJob.getCounters();
  }

  private void parseArgs() {
    Properties prop = System.getProperties();

    String runmode = prop.getProperty("odps.runner.mode");
    if (runmode != null && runmode.length() != 0) {
      SessionState.get().setLocalRun(runmode.equalsIgnoreCase("local"));
    }

    String resources = prop.getProperty("odps.cache.resources");
    if (resources != null && !resources.trim().isEmpty()) {
      this.addCacheResourcesToClassPath(resources);
    }

    String project = prop.getProperty("odps.project.name");

    if (prop.getProperty("odps.access.id") != null) {
      String endpoint = prop.getProperty("odps.end.point");
      String accessId = prop.getProperty("odps.access.id");
      String accessKey = prop.getProperty("odps.access.key");

      Account account = new AliyunAccount(accessId, accessKey);
      Odps odps = new Odps(account);
      odps.setDefaultProject(project);
      if (endpoint != null && endpoint.length() != 0) {
        odps.setEndpoint(endpoint);
      }

      String logViewHost = prop.getProperty("odps.logview.host");
      if (logViewHost != null && logViewHost.length() != 0) {
        odps.setLogViewHost(logViewHost);
      }

      SessionState.get().setOdps(odps);
    }
    // if in local mode and no odps in sessionState, fill it.
    else if (SessionState.get().isLocalRun() &&
             SessionState.get().getOdps() == null) {
      Account account = new AliyunAccount("defaultId", "defaultKey");
      Odps odps = new Odps(account);
      odps.setDefaultProject(project);
      SessionState.get().setOdps(odps);
    }
  }
}
