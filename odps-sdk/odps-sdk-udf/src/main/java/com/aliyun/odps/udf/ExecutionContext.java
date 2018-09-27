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

package com.aliyun.odps.udf;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.volume.FileSystem;

/**
 * 运行时的执行上下文信息。
 * <p>
 * 包括UDF所在的StageID，WorkerID等信息。
 * </p>
 */
public abstract class ExecutionContext {

  protected String stageID;
  protected int workerID;
  protected String tableInfo;
  protected String localResourceDirectory;

  /**
   * 获取运行时StageID
   *
   * @return StageID
   */
  public String getStageID() {
    return stageID;
  }

  /**
   * 获取运行时WorkerID
   *
   * @return WorkerID
   */
  public int getWorkerID() {
    return workerID;
  }

  /**
   * 获取运行时处理的数据块所属的表或分区信息。
   *
   * @return <p>非分区表返回："项目名.表名"。分区表返回："项目名.表名/分区名=分区值"。如果有多级分区，按照分区结构返回相应的目录结构。</p>
   * @throws <p>当UDF运行在非Map端时，调用此方法会抛出
   *     {@link InvalidInvocationException} 异常。</p>
   */
  public String getTableInfo() {
    if (tableInfo == null) {
      throw new InvalidInvocationException("table info is only available at mapper.");
    }
    return tableInfo;
  }

  /**
   * 在{@link UDF}运行期调用此方法防止worker因超时被强制终止。
   * <p>
   * {@link UDF}在运行期每一次调用({@link UDF#evaluate()}、{@link UDTF#process(Object[])}等方法)
   * 最多能消耗的时间有一定限制，当超出这个限制时，worker会被强制终止。
   * 当{@link UDF}涉及计算密集型的任务时，可能一次调用需要消耗更多时间，调用此方法可以重置时间限制，防止被提前终止。
   * </p>
   */
  public abstract void claimAlive();

  private final static int MAX_COUNTERS_NUM = 64;
  private final static int MAX_COUNTERS_LEN = 100;
  
  protected int countersNumberLimit = MAX_COUNTERS_NUM;
  protected Counters counters = new Counters();
  protected HashMap<String, Counter> userCounters = new HashMap<String, Counter>();

  /**
   * 通过{@link Enum}获取{@link Counter}
   * <p>
   * 等价于调用<pre>
   * {@code
   * getCounter(name.getDeclaringClass().getName(), name.toString())
   * }
   * </pre>
   * </p>
   *
   * @param name
   *     {@link Counter}标识
   * @return 对应标识的{@link Counter}对象
   * @throws IllegalArgumentException
   *     - name 不能为null
   * @see #getCounter(String, String)
   */
  public Counter getCounter(Enum<?> name) {
    if (name == null) {
      throw new IllegalArgumentException("Counter name must not be null.");
    }
    return getCounter(name.getDeclaringClass().getName(), name.toString());
  }

  /**
   * 通过Group名和Counter名获取{@link Counter}
   * <p>
   * {@link Counter}信息最后会汇总在SQL Task的Summary中。
   * </p>
   *
   * @param group
   *     Group标识
   * @param name
   *     Counter标识
   * @return 对应标识的{@link Counter}对象
   * @throws IllegalArgumentException
   *     Group名和Counter名中不能 <br>
   *     <ul>
   *     <li>为null</li>
   *     <li>为空字符串 </li>
   *     <li>包含'#'字符 </li>
   *     <li>加起来字符串长度超过64字节</li>
   *     </ul>
   *     <p>
   *     当所定义Counter数量超过100个时也会抛出异常
   *     </p>
   */
  public Counter getCounter(String group, String name) {
    String key = group + "#" + name;
    if (userCounters.containsKey(key)) {
      return userCounters.get(key);
    }
    checkUserDefinedCounters(group, name);
    Counter counter = counters.findCounter(group, name);
    userCounters.put(key, counter);
    return counter;
  }

  private void checkUserDefinedCounters(String groupName, String counterName) {
    if (groupName == null || groupName.isEmpty() ||
        counterName == null || counterName.isEmpty()) {
      throw new IllegalArgumentException("Group or counter name must not be null or empty.");
    }
    if (groupName.contains("#")) {
      throw new IllegalArgumentException("Group name: '" + shortenName(groupName)
                                         + "' cannot contain character '#'");
    }
    if (counterName.contains("#")) {
      throw new IllegalArgumentException("Counter name: '" + shortenName(counterName)
                                         + "' cannot contain character '#'");
    }
    int maxLength = MAX_COUNTERS_LEN;
    if (groupName.length() + counterName.length() > maxLength) {
      throw new IllegalArgumentException("Length of group name '" + shortenName(groupName)
                                         + "' and counter name '" + shortenName(counterName)
                                         + "' exceeded limit " + maxLength);
    }
    if (userCounters.size() >= countersNumberLimit) {
      throw new IllegalArgumentException(
          "Total num of user defined counters exceeded limit "
          + countersNumberLimit);
    }
  }

  private String shortenName(String name) {
    if (name.length() > 110) {
      return name.substring(0, 110) + "...";
    }
    return name;
  }

  /**
   * 读取文件类型资源，返回一个带缓存的输入流。
   *
   * <p>
   * {@link #readResourceFileAsStream(String)}
   * 该方法支持边读边处理（常见的场景是读取一行处理一行），适合读取比较大的文件资源，
   * 防止Java内存溢出，如果文件资源比较小，也可以直接使用。<br />
   * {@link #readResourceFile(String)} 方法会把内容一次性读取到内存。
   * </p>
   *
   * @param resourceName
   *     资源名称
   * @return 资源内容的BufferedInputStream
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   * @see #readResourceFile(String)
   */
  public abstract BufferedInputStream readResourceFileAsStream(
      String resourceName) throws IOException;

  /**
   * 读取文件类型资源，一次全部读取到内存，返回 byte[].
   *
   * <p>
   * 如果资源文件比较大，应该使用{@link #readResourceFileAsStream(String)}
   * 获得一个带缓存的输入流，支持边读边处理，防止Java内存溢出。
   * </p>
   *
   * @param resourceName
   *     资源名称
   * @return 资源内容
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   * @see #readResourceFileAsStream(String)
   */
  public abstract byte[] readResourceFile(String resourceName)
      throws IOException;

  /**
   * <p>
   * 读取表类型资源，返回Iterable<Object[]>，Object为对应表每一列的Java原生类型.
   * </p>
   *
   * @param resourceName
   *     资源名称
   * @return Iterable<Object[]>
   * @throws IOException
   *     资源未声明、资源类型不匹配以及其他读取错误抛异常
   */
  public abstract Iterable<Object[]> readResourceTable(String resourceName)
      throws IOException;

  /**
   * 读取压缩档案类型资源，返回 BufferedInputStream 的迭代器.
   *
   * @param resourceName 资源名称
   * @return BufferedInputStream的迭代器
   * @throws IOException 资源未声明、资源类型不匹配以及其他读取错误抛异常
   */
  public abstract Iterable<BufferedInputStream> readCacheArchiveAsStream(String resourceName)
      throws IOException;

  /**
   * 读取压缩档案类型资源，返回 BufferedInputStream 的迭代器.
   *
   * @param resourceName
   *        资源名称
   * @param relativePath
   *        读取资源的相对路径
   * @return BufferedInputStream的迭代器
   * @throws IOException 资源未声明、资源类型不匹配以及其他读取错误抛异常
   */
  public abstract Iterable<BufferedInputStream> readCacheArchiveAsStream(String resourceName,
      String relativePath) throws IOException;

  public abstract VolumeInfo getInputVolumeInfo() throws IOException;

  public abstract VolumeInfo getInputVolumeInfo(String label) throws IOException;

  public abstract VolumeInfo getOutputVolumeInfo() throws IOException;

  public abstract VolumeInfo getOutputVolumeInfo(String label) throws IOException;

  public abstract FileSystem getInputVolumeFileSystem() throws IOException;

  public abstract FileSystem getInputVolumeFileSystem(String label) throws IOException;

  public abstract FileSystem getOutputVolumeFileSystem() throws IOException;

  public abstract FileSystem getOutputVolumeFileSystem(String label) throws IOException;

  public abstract Properties getConfigurations();

  public abstract FileSystem getTempFileSystem() throws IOException;
}
