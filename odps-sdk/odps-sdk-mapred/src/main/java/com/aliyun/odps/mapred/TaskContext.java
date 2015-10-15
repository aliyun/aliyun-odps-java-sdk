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

package com.aliyun.odps.mapred;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.volume.FileSystem;

/**
 * MapReduce任务运行时的上下文信息
 */
public interface TaskContext extends JobContext {

  /**
   * 获取当前运行Task的标识.
   *
   * @return 当前运行{@link TaskId} 的标识
   */
  TaskId getTaskID();

  /**
   * 获取默认输出的表信息.
   *
   * @return 默认输出的表信息 {@link TableInfo}
   * @throws IOException
   */
  TableInfo[] getOutputTableInfo() throws IOException;

  /**
   * 创建默认输出表的记录对象，创建的记录对象会包含表schema信息.
   *
   * <p>
   * 最好在{@link com.aliyun.odps.mapred.Mapper#setup(TaskContext)}和
   * {@link com.aliyun.odps.mapred.Reducer#setup(TaskContext)}
   * 方法里创建输出记录对象然后重用，避免频繁进行输出记录对象的创建。
   *
   * <p>
   * 代码示例：
   *
   * <pre>
   * private Record result;
   *
   * &#064;Override
   * public void setup(TaskContext context) throws IOException {
   *   result = context.createOutputRecord();
   * }
   *
   * &#064;Override
   * public void reduce(Record key, Iterator&lt;Record&gt; values, TaskContext context)
   *     throws IOException {
   *   long count = 0;
   *   while (values.hasNext()) {
   *     Record val = values.next();
   *     count += (Long) val.get(0);
   *   }
   *   result.set(0, key.get(0));
   *   result.set(1, count);
   *   context.write(result);
   * }
   *
   * </pre>
   *
   * </p>
   *
   * @return 默认输出表的记录对象
   * @throws IOException
   * @see #write(Record)
   */
  Record createOutputRecord() throws IOException;

  /**
   * 创建给定label输出表的记录对象，创建的记录对象会包含表schema信息.
   *
   * <p>
   * 最好在{@link com.aliyun.odps.mapred.Mapper#setup(TaskContext)}和
   * {@link com.aliyun.odps.mapred.Reducer#setup(TaskContext)}
   * 方法里创建输出记录对象然后重用，避免频繁进行输出记录对象的创建。
   *
   * <p>
   * 代码示例：
   *
   * <pre>
   * private Map&lt;String, Record&gt; result;
   *
   * &#064;Override
   * public void setup(TaskContext context) throws IOException {
   *   TableInfo[] tbls = context.getOutputTableInfo();
   *   result = new HashMap&lt;String, Record&gt;();
   *   for (TableInfo tbl : tbls) {
   *     String lable = tbl.getLabel();
   *     result.put(lable, context.createOutputRecord(lable));
   *   }
   * }
   *
   * &#064;Override
   * public void reduce(Record key, Iterator&lt;Record&gt; values, TaskContext context)
   *     throws IOException {
   *   long count = 0;
   *   while (values.hasNext()) {
   *     Record val = values.next();
   *     count += (Long) val.get(0);
   *   }
   *
   *   for (String lable : result.keySet()) {
   *     Record r = result.get(lable);
   *     r.set(0, key.get(0));
   *     r.set(1, count);
   *     context.write(r, lable);
   *   }
   * }
   * </pre>
   *
   * </p>
   *
   * @param label
   *     输出标签
   * @return 给定label输出表的记录对象
   * @throws IOException
   * @see #write(Record, String)
   */
  Record createOutputRecord(String label) throws IOException;

  Record createOutputKeyRecord() throws IOException;

  Record createOutputValueRecord() throws IOException;

  /**
   * 创建Map输出Key的记录对象.
   *
   * @return {@link Record}
   * @throws IOException
   */
  Record createMapOutputKeyRecord() throws IOException;

  /**
   * 创建Map输出Value的记录对象
   *
   * @return {@link Record}
   * @throws IOException
   */
  Record createMapOutputValueRecord() throws IOException;

  /**
   * 读取文件类型资源，返回一个带缓存的输入流.
   *
   * <p>
   * readResourceFileAsStream 支持边读边处理
   * </p>
   *
   * @param resourceName
   *     资源名称
   * @return 资源内容的{@link BufferedInputStream}
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   */
  BufferedInputStream readResourceFileAsStream(String resourceName)
      throws IOException;
  
  /**
   * 读取Archive文件类型资源，返回一个带缓存的输入流迭代器.
   * 
   * <p>
   * readResourceArchiveAsStream 支持边读边处理
   * </p>
   * 
   * @param resourceName 资源名称
   * @return 资源内容的{@link BufferedInputStream}的迭代器
   * @throws IOException 资源未声明、资源类型不匹配以及其他读取错误抛异常
   */
  Iterable<BufferedInputStream> readResourceArchiveAsStream(String resourceName) throws IOException;

  /**
   * 读取Archive文件类型资源，返回一个带缓存的输入流的迭代器.
   * 
   * <p>
   * readResourceArchiveAsStream 支持边读边处理
   * </p>
   * 
   * @param resourceName 资源名称
   * @param relativePath Archive内部相对路径
   * @return 资源内容的{@link BufferedInputStream}的迭代器
   * @throws IOException 资源未声明、资源类型不匹配以及其他读取错误抛异常
   */
  Iterable<BufferedInputStream> readResourceArchiveAsStream(String resourceName, String relativePath)
      throws IOException;

  /**
   * 读取表类型资源，ODPS的小表（Table）也可以作为资源，大小限制参见ODPS的相关文档.
   *
   * <p>
   * 使用表类型资源步骤：<br/>
   * <ol>
   * <li>增加资源表： console < add table my_table partition(ds='1') as res_table;
   * <li>运行M/R作业命令: console < jar -resources res_table ...，或者使用API（
   * {@link JobConf#setResources(String)}）
   * <li>M/R里读取资源：context.readResourceTable("res_table");
   * </ol>
   * </p>
   *
   * <p>
   * 代码示例：
   *
   * <pre>
   * Iterator&lt;Record&gt; iter = context.readResourceTable(&quot;res_table&quot;);
   * while (iter.hasNext()) {
   *   Record r = iter.next();
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
  Iterator<Record> readResourceTable(String resourceName)
      throws IOException;

  /**
   * 获取给定名称的Counter对象，name为{@link Enum}
   *
   * @param name
   *     Counter名称
   * @return Counter
   */
  Counter getCounter(Enum<?> name);

  /**
   * 获取给定组名和名称的 Counter 对象.
   *
   * @param group
   *     Counter 组名
   * @param name
   *     Counter 名
   * @return Counter
   */
  Counter getCounter(String group, String name);

  /**
   * 向 MapReduce 框架报告进度.
   *
   * 如果用户方法处理时间很长，且中间没有调用框架，可以调用这个方法避免task超时，框架默认600秒超时.
   */
  void progress();

  /**
   * 写记录到默认输出.
   *
   * @param {@link
   *     Record}
   *     待写出的记录对象
   * @throws IOException
   * @see #createOutputRecord()
   */
  public void write(Record record) throws IOException;

  /**
   * 写记录到给定标签输出.
   *
   * @param record
   *     待写出的记录对象
   * @throws IOException
   * @see #createOutputRecord(String)
   */
  public void write(Record record, String label) throws IOException;

  /**
   * Map写记录到中间结果
   *
   * @param key
   * @param value
   * @throws IOException
   */
  public void write(Record key, Record value) throws IOException;

  /**
   * 获得默认的输入volume的描述信息
   *
   * @return VolumeInfo
   * @throws IOException
   */
  VolumeInfo getInputVolumeInfo() throws IOException;

  /**
   * 获得给定label的输入volume的描述信息
   *
   * @return VolumeInfo
   * @throws IOException
   */
  VolumeInfo getInputVolumeInfo(String label) throws IOException;

  /**
   * 获取默认的输出volume的描述信息
   *
   * @return VolumeInfo
   * @throws IOException
   */
  VolumeInfo getOutputVolumeInfo() throws IOException;

  /**
   * 获取给定label的输出volume的描述信息
   *
   * @param label
   * @return VolumeInfo
   * @throws IOException
   */
  VolumeInfo getOutputVolumeInfo(String label) throws IOException;

  /**
   * 获取默认的输入volume fileSystem
   *
   * @return FileSystem
   * @throws IOException
   */
  FileSystem getInputVolumeFileSystem() throws IOException;

  /**
   * 获取给定label的输入volume fileSystem
   *
   * @param label
   *     输出label
   * @return FileSystem
   * @throws IOException
   */
  FileSystem getInputVolumeFileSystem(String label) throws IOException;

  /**
   * 获取默认的输出volume fileSystem
   *
   * @return FileSystem
   * @throws IOException
   */
  FileSystem getOutputVolumeFileSystem() throws IOException;

  /**
   * 获取给定label的输出volume fileSystem
   *
   * @param label
   *     输出label
   * @return FileSystem
   * @throws IOException
   */
  FileSystem getOutputVolumeFileSystem(String label) throws IOException;
}
