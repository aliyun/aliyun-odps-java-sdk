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

import java.io.BufferedInputStream;
import java.io.IOException;

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.io.WritableRecord;

/**
 * Worker 上下文对象，封装了框架提供的核心功能.
 *
 * @param <VERTEX_ID>
 *     Vertex ID 类型
 * @param <VERTEX_VALUE>
 *     Vertex Value 类型
 * @param <EDGE_VALUE>
 *     Edge Value 类型
 * @param <MESSAGE>
 *     Message 类型
 */
@SuppressWarnings("rawtypes")
public abstract class WorkerContext<VERTEX_ID extends WritableComparable, VERTEX_VALUE extends Writable, EDGE_VALUE extends Writable, MESSAGE extends Writable> {

  /**
   * 获取作业配置.
   *
   * 在 {@link GraphJob} 设置的配置参数可以从作业配置中读取
   *
   * <p>
   * 注意：这里只能get配置参数，而不能set配置参数
   * </p>
   *
   * @return ODPS Graph 作业配置
   */
  public abstract Configuration getConfiguration();

  /**
   * 获取当前 Worker 的共享对象.
   *
   * @return 当前 Worker 的共享对象.
   * @see {@link WorkerComputer}
   */
  public abstract Writable getWorkerValue();

  /**
   * 获取当前 Worker 的编号，从 0 开始计数.
   *
   * @return 当前 Worker 的编号，从 0 开始计数.
   */
  public abstract int getWorkerId();

  /**
   * 获取 Worker 数目.
   *
   * @return Worker 数目
   */
  public abstract int getNumWorkers();

  /**
   * 获取当前超步，即第几次迭代，从 0 开始计数.
   *
   * @return 当前超步
   */
  public abstract long getSuperstep();

  /**
   * 获取最大迭代次数，默认 -1，小于或等于 0 时表示最大迭代次数不作为作业终止条件.
   *
   * @return 最大迭代次数
   * @see JobConf#setMaxIteration(int)
   */
  public abstract long getMaxIteration();

  /**
   * 获取当前 Worker 负责的点数量.
   *
   * @return 当前 Worker 负责的点数量
   */
  public abstract long getWorkerNumVertices();

  /**
   * 获取当前 Worker 负责的边数量.
   *
   * @return 当前 Worker 负责的边数量
   */
  public abstract long getWorkerNumEdges();

  /**
   * 获取所有点数量，所有 Worker 负责的点数之和.
   *
   * @return 所有点数量
   */
  public abstract long getTotalNumVertices();

  /**
   * 获取所有边数量，所有 Worker 负责的边数之和.
   *
   * @return 所有边数量
   */
  public abstract long getTotalNumEdges();

  /**
   * 获取给定名称的Counter对象，name为{@link Enum}
   *
   * @param name
   *     Counter名称
   * @return
   */
  public abstract Counter getCounter(Enum<?> name);

  /**
   * 获取给定组名和名称的 Counter 对象.
   *
   * @param group
   *     Counter 组名
   * @param name
   *     Counter 名
   * @return
   */
  public abstract Counter getCounter(String group, String name);

  /**
   * 获取上一轮迭代结束时给定编号 Aggregator 的结果.
   *
   * <p>
   * 对于第一轮迭代（超步为0），调用本方法返回 {@link Aggregator#createStartupValue(WorkerContext)}
   * 创建的值， {@link Aggregator#createStartupValue(WorkerContext)} 默认返回 null，用户可以
   * override.
   *
   * @param <VALUE>
   *     Aggregator 值类型
   * @param aggregatorIndex
   *     Aggregator编号，起始为 0
   * @return
   * @see Aggregator#aggregate(Writable, Object)
   * @see GraphJob#setAggregatorClass(Class...)
   */
  public abstract <VALUE extends Writable> VALUE getLastAggregatedValue(
      int aggregatorIndex);

  /**
   * 默认获取上一轮迭代结束时编号0的 Aggregator 的结果.
   *
   * <p>
   * 对于第一轮迭代（超步为0），调用本方法返回 {@link Aggregator#createStartupValue(WorkerContext)}
   * 创建的值， {@link Aggregator#createStartupValue(WorkerContext)} 默认返回 null，用户可以
   * override.
   *
   * @param <VALUE>
   *     Aggregator 值类型
   * @return
   * @see Aggregator#aggregate(Writable, Object)
   * @see GraphJob#setAggregatorClass(Class...)
   */
  public <VALUE extends Writable> VALUE getLastAggregatedValue() {
    return getLastAggregatedValue(0);
  }

  /**
   * 获取默认输出的表信息.
   *
   * @return 默认输出的表信息
   * @throws IOException
   */
  public abstract TableInfo getOutputTable() throws IOException;

  /**
   * 获取给定输出标签的表信息.
   *
   * @param label
   *     输出标签
   * @return 给定输出标签的表信息
   * @throws IOException
   */
  public abstract TableInfo getOutputTable(String label) throws IOException;

  /**
   * 写记录到默认输出.
   *
   * @param fieldVals
   *     待写出的记录值
   * @throws IOException
   * @see GraphJob#addOutput(TableInfo)
   */
  public abstract void write(Writable... fieldVals) throws IOException;

  /**
   * 写记录到给定标签输出.
   *
   * @param label
   *     输出标签
   * @param fieldVals
   *     待写出的记录值
   * @throws IOException
   * @see GraphJob#addOutput(TableInfo, String)
   */
  public abstract void write(String label, Writable... fieldVals)
      throws IOException;

  /**
   * 读取文件类型资源，一次全部读取到内存，返回 byte[].
   *
   * <p>
   * 如果资源文件比较大，应该使用{@link #readCacheFileAsStream(String)}
   * 获得一个带缓存的输入流，支持边读边处理，防止Java内存溢出
   * </p>
   *
   * <p>
   * 使用文件类型资源步骤：<br/>
   * <ol>
   * <li>上传资源文件： console < add file res_file.txt;
   * <li>运行 ODPS Graph 作业命令: console < jar -resources res_file.txt ...，或者使用API（
   * {@link JobConf#addCacheResources(String)}）
   * <li>ODPS Graph 作业里读取资源：context.readCacheFile("res_file.txt");
   * </ol>
   * </p>
   *
   * <p>
   * 资源文件在运行时会下载到所在的节点机器上并缓存，类似于Hadoop的<a href=
   * "http://hadoop.apache.org/common/docs/r0.19.2/cn/mapred_tutorial.html#DistributedCache"
   * >DistributedCache</a>
   * </p>
   *
   * @param resourceName
   *     资源名称
   * @return 资源内容
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   * @see #readCacheFileAsStream(String)
   */
  public abstract byte[] readCacheFile(String resourceName) throws IOException;

  /**
   * 读取文件类型资源，返回一个带缓存的输入流.
   *
   * <p>
   * readCacheFileAsStream
   * 支持边读边处理（常见的场景是读取一行处理一行），适合读取比较大的文件资源，防止Java内存溢出，如果文件资源比较小，也可以直接使用
   * {@link #readCacheFile(String)} 方法一次性读取到内存。
   * </p>
   *
   * <p>
   * 使用文件类型资源步骤类似{@link #readCacheFile(String)}
   * </p>
   *
   * @param resourceName
   *     资源名称
   * @return 资源内容的BufferedInputStream
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   * @see #readCacheFile(String)
   */
  public abstract BufferedInputStream readCacheFileAsStream(String resourceName)
      throws IOException;

  /**
   * 读取压缩档案类型资源，返回解压后的所有文件内容的迭代器.
   *
   * <p>
   * 如果文件内容比较大，应该使用 {@link #readCacheArchiveAsStream(String)}
   * ，支持边读边处理，防止Java内存溢出。
   * </p>
   *
   * <p>
   * 使用压缩档案类型资源步骤：<br/>
   * <ol>
   * <li>上传资源压缩包： console < add archive ref_archive.tar.gz;
   * <li>运行 ODPS Graph 作业命令: console < jar -resources ref_archive.tar.gz
   * ...，或者使用API（ {@link JobConf#addCacheResources(String)}）
   * <li>ODPS Graph 里读取资源：context.readCacheArchive("ref_archive.tar.gz");
   * </ol>
   * </p>
   *
   * <p>
   * 资源压缩包在Worker运行时会下载到所在的节点机器上并缓存，类似于Hadoop的<a href=
   * "http://hadoop.apache.org/common/docs/r0.19.2/cn/mapred_tutorial.html#DistributedCache"
   * >DistributedCache</a>
   * </p>
   *
   * @param resourceName
   *     资源名称
   * @return 压缩包解压后的所有文件内容迭代器
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   * @see #readCacheArchiveAsStream(String)
   */
  public abstract Iterable<byte[]> readCacheArchive(String resourceName)
      throws IOException;

  /**
   * 读取压缩档案类型资源，可以传入一个相对路径读取部分文件.
   *
   * <p>
   * 与{@link #readCacheArchive(String)} 不同的是可以传入一个相对路径，用于读取压缩包里的部分文件的内容，更多说明见
   * {@link #readCacheArchive(String)}方法
   * </p>
   *
   * <p>
   * 读取压缩包内子目录的代码示例：
   *
   * <pre>
   * Iterable&lt;byte[]&gt; iter = context.readCacheArchiveAsStream(RESOURCE_ARCHIVE,
   *     &quot;subdir/*&quot;);
   * for (byte[] content : iter) {
   * }
   * </pre>
   *
   * </p>
   *
   * @param resourceName
   *     资源名称
   * @param relativePath
   *     读取资源的相对路径
   * @return 文件内容迭代器
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   * @see #readCacheArchiveAsStream(String)
   */
  public abstract Iterable<byte[]> readCacheArchive(String resourceName,
                                                    String relativePath) throws IOException;

  /**
   * 读取压缩档案类型资源，返回 BufferedInputStream 的迭代器.
   *
   * @param resourceName
   *     资源名称
   * @return BufferedInputStream的迭代器
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   * @see #readCacheArchive(String)
   */
  public abstract Iterable<BufferedInputStream> readCacheArchiveAsStream(
      String resourceName) throws IOException;

  /**
   * 读取压缩档案类型资源，返回 BufferedInputStream 的迭代器.
   *
   * @param resourceName
   *     资源名称
   * @param relativePath
   *     读取资源的相对路径
   * @return BufferedInputStream的迭代器
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   * @see #readCacheArchive(String, String)
   */
  public abstract Iterable<BufferedInputStream> readCacheArchiveAsStream(
      String resourceName, String relativePath) throws IOException;

  /**
   * 读取表类型资源，ODPS的小表（Table）也可以作为资源，大小限制参见ODPS的相关文档.
   *
   * <p>
   * 使用表类型资源步骤：<br/>
   * <ol>
   * <li>增加资源表： console < add table my_table partition(ds='1') as res_table;
   * <li>运行 ODPS Graph 作业命令: console < jar -resources res_table ...，或者使用API（
   * {@link JobConf#addCacheResources(String)}）
   * <li>ODPS Graph 里读取资源：context.readResourceTable("res_table");
   * </ol>
   * </p>
   *
   * <p>
   * 代码示例：
   *
   * <pre>
   * Iterable&lt;WritableRecord&gt; iter = context.readResourceTable(&quot;res_table&quot;);
   * for (WritableRecord record : iter) {
   *   // handle record
   * }
   * </pre>
   *
   * @param resourceName
   *     表类型资源名称
   * @return 记录迭代器
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他错误抛异常
   */
  public abstract Iterable<WritableRecord> readResourceTable(String resourceName)
      throws IOException;

  /**
   * 获取表类型资源对应的表信息，包括Project、表名称、分区等信息.
   *
   * @param resourceName
   *     表类型资源名称
   * @return 表信息
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他错误抛异常
   */
  public abstract TableInfo getResourceTable(String resourceName)
      throws IOException;

  /**
   * 向 ODPS Graph 框架报告进度.
   *
   * 如果用户方法处理时间很长，且中间没有调用框架，可以调用这个方法避免worker超时，框架默认600秒超时.
   */
  public abstract void progress();

  /**
   * 获取当前 Worker 的计算对象，每轮开始时初始化，结束时发送给 Master。
   * 在 {@link MasterComputer} 中能获取到各个 Worker 的 ComputeValue.
   *
   * @return 返回当前 Worker 的计算对象
   */
  public abstract Writable getComputeValue();
}
