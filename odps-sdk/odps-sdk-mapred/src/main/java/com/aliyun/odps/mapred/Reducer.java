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

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;

/**
 * Reducer 对与一个键（Key）关联的一组数值集（Values）进行归约计算.
 *
 * <p>
 * Reducer 由{@link com.aliyun.odps.mapred.conf.JobConf#setReducerClass(Class)}
 * 进行指定.<br/>
 * Reducer 任务数由
 * {@link com.aliyun.odps.mapred.conf.JobConf#setNumReduceTasks(int)}
 * 进行指定，用户需要根据 Reducer 要处理的数据量（{@link Mapper} 的输出）进行调整，若没有指定 Reducer 任务数，默认设置为
 * {@link Mapper} 数的 1/4<br/>
 * </p>
 *
 * <p>
 * 与 {@link Mapper} 类似，Reducer 也包括三个方法：
 * <ul>
 * <li>{@link #setup(TaskContext)} 在任务开始， reduce 方法之前调用，每个reduce调用且仅调用一次
 * <li>{@link #cleanup(TaskContext)} 在任务结束，reduce 方法之后调用，每个reduce调用且仅调用一次
 * <li>{@link #reduce(Record, Iterator, TaskContext)} reduce 方法，每次调用传入一个 Key
 * 和与之关联的一组 Value集合
 * </ul>
 * 上述方法都会传入一个上下文对象，Reducer 实现时可以调用上下文对象的方法，编写代码时最好能多参考下 {@link TaskContext} 接口<br/>
 *
 * <br/>
 * <strong><font color="red"> 注意：传给 reduce 方法的 Key key 和 Iterator<VALUEIN>
 * values 里的Value对象都会被上下文对象复用。如果需要保存Record的数据，使用{@link Record#toArray}
 * </font></strong> <br/>
 *
 * </p>
 *
 * <p>
 * 在{@link Mapper} 端，每个 mapper 生成中间结果（键值对）并通过Partitioner Columns 哈希分发给对应的
 * Reducer，Reducer 端，每个 Reducer 会读入所有 {@link Mapper} 输出给它的数据，这个过程分三步：<br/>
 * <ol>
 * <li>归并排序：Reducer 的输入虽然每一路都有序，但是整体并未有序，这时 Reducer 会进行归并排序，排序使用的比较列（Sort
 * Columns）可以由
 * {@link com.aliyun.odps.mapred.conf.JobConf#setOutputKeySortColumns(String[])}
 * 进行指定，如果没有指定，使用 key中的所有列做排序。
 * <li>按 Key 对 Values 分组：经过第一步的归并排序，所有的 Key/Value 对都已经有序，这是会使用
 * {@link com.aliyun.odps.mapred.conf.JobConf#setOutputGroupingColumns(String[])}
 * 指定的分组列对排好序的 Key/Value 对进行分组，如果没有指定，则默认使用key中的所有列做分组。
 * <li>循环调用 reduce 方法，传入 Key 和 Value迭代器进行规约计算。
 * </ol>
 * </p>
 *
 *
 * <p>
 * 计算结果可以通过下面的方法输出到结果表：
 * <ul>
 * <li>{@link TaskContext#write(Record)} 输出计算结果到默认输出表
 * <li>{@link TaskContext#write(Record, String)} 输出计算结果到指定 label 的输出表
 * </ul>
 * </p>
 *
 * <p>
 * 在客户端定义的作业配置（{@link com.aliyun.odps.mapred.conf.JobConf}）可以通过
 * {@link TaskContext#getJobConf()} 方法取得。
 * </p>
 *
 * <p>
 * 代码示例，摘自 WordCount:
 *
 * <pre>
 * public static class SumReducer extends ReducerBase {
 *   private Record result;
 *
 *   &#064;Override
 *   public void setup(TaskContext context) throws IOException {
 *     result = context.createOutputRecord();
 *   }
 *
 *   &#064;Override
 *   public void reduce(Record key, Iterator&lt;Record&gt; values, TaskContext context) throws
 * IOException {
 *     long count = 0;
 *     while (values.hasNext()) {
 *       Record val = values.next();
 *       count += (Long) val.get(0);
 *     }
 *     result.set(0, key.get(0));
 *     result.set(1, count);
 *     context.write(result);
 *   }
 * }
 * </pre>
 *
 * </p>
 *
 * @see TaskContext
 * @see Mapper
 */
public interface Reducer {

  /**
   * 传给{@link Reducer}的上下文对象.
   */
  public interface TaskContext extends com.aliyun.odps.mapred.TaskContext {

    /**
     * 向前读取下一个Key value对象，如果还有未读取的Key value，返回true，否则返回false
     *
     * 读取的Key对象可以通过{@link #getCurrentKey()}取得
     *
     * @return 如果还有未读取的Key，返回true，否则返回false
     */
    public boolean nextKeyValue();

    /**
     * 获取当前Key对象
     *
     * 注意：此方法返回的Key对象在{@link #nextKey()}方法里会被复用
     *
     * @return 当前Key对象
     */
    public Record getCurrentKey();

    /**
     * 返回当前Key对应的所有Value的迭代器
     *
     * 注意：迭代器里的Value对象会被复用
     *
     * @return 当前Key对应的所有Value的迭代器
     */
    public Iterator<Record> getValues();
  }

  /**
   * 在任务开始， reduce 方法之前调用.
   *
   * @param context
   *     Reduce 上下文对象
   * @throws IOException
   */
  void setup(TaskContext context) throws IOException;

  /**
   * 对每个 Key 和与其关联的一组Value集合进行处理.
   *
   *
   * <p>
   * <strong><font color="red"> 注意：传给 reduce 方法的 Key key 和 Iterator<Record>
   * values 里的Value对象都会被上下文对象复用！</font></strong> <br/>
   * </p>
   *
   * @param key
   *     键
   * @param values
   *     与 key 关联的 value 迭代器
   * @param context
   *     Reduce 上下文对象
   * @throws IOException
   */
  void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException;

  /**
   * 在任务结束， reduce 方法之后调用，可以重载此方法.
   *
   * @param context
   *     Reduce 上下文对象
   * @throws IOException
   */
  void cleanup(TaskContext context) throws IOException;
}
