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
 * MutationContext 定义了支持图拓扑变化的接口，包括增加/删除点或边.
 *
 * <p>
 * MutationContext 接口可以在两个用户自定义方法中使用：
 * <ul>
 * <li>
 * {@link GraphLoader#load(com.aliyun.odps.io.LongWritable, com.aliyun.odps.Record,
 * MutationContext)}
 * 用于加载图；
 * <li>{@link Vertex#compute(ComputeContext, Iterable)}
 * 在每一轮超步中用于修改图的拓扑结构，图拓扑结构变化将在下一轮超步生效；
 * </ul>
 * </p>
 *
 * <p>
 * 图拓扑结构的变化可能存在冲突，例如增加两个相同 ID 的点等等，可以提供冲突解决类自定义冲突处理方法：
 * <ul>
 * <li>
 * {@link GraphJob#setLoadingVertexResolverClass(Class)} 设置图加载过程的自定义冲突处理类；
 * <li>
 * {@link GraphJob#setComputingVertexResolverClass(Class)} 设置图迭代过程的自定义冲突处理类；
 * </ul>
 * </p>
 *
 * @param <I>
 *     Vertex ID 类型
 * @param <V>
 *     Vertex Value 类型
 * @param <E>
 *     Edge Value 类型
 * @param <M>
 *     Message 类型
 * @see VertexResolver
 */
@SuppressWarnings("rawtypes")
public interface MutationContext<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable> {

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
   * 获取 Worker 数目.
   *
   * @return Worker 数目
   */
  public abstract int getNumWorkers();


  /**
   * 增加点请求，下一轮生效.
   *
   * <p>
   * 冲突处理需要提供 {@link VertexResolver}
   *
   * @param vertex
   *     待增加点的 Vertex 对象，应保证该对象不被复用，包括对象的成员变量
   * @throws IOException
   */
  public void addVertexRequest(Vertex<I, V, E, M> vertex) throws IOException;

  /**
   * 删除点请求，下一轮生效.
   *
   * <p>
   * 冲突处理需要提供 {@link VertexResolver}
   *
   * @param vertexId
   *     待删除点的 ID
   * @throws IOException
   */
  public void removeVertexRequest(I vertexId) throws IOException;

  /**
   * 增加边请求，下一轮生效。
   *
   * <p>
   * 冲突处理需要提供 {@link VertexResolver}
   *
   * @param sourceVertexId
   *     待增加边的起点
   * @param edge
   *     待增加边，应保证该对象不被复用，包括对象的成员变量
   * @throws IOException
   */
  public abstract void addEdgeRequest(I sourceVertexId, Edge<I, E> edge)
      throws IOException;

  /**
   * 删除边请求，下一轮生效。
   *
   * <p>
   * 冲突处理需要提供 {@link VertexResolver}
   *
   * @param sourceVertexId
   *     待删除边的起点
   * @param targetVertexId
   *     待删除边的终点，应保证该对象不被复用
   * @throws IOException
   */
  public void removeEdgeRequest(I sourceVertexId, I targetVertexId)
      throws IOException;

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

}
