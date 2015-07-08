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

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.JobConf;

/**
 * Mapper 处理输入表的记录对象 {@link Record}，加工处理成键值对集合（若 {@link Reducer} 个数不为0）输出到
 * {@link Reducer} ，或者直接输出结果记录.
 *
 * <p>
 * Mapper 由{@link JobConf#setMapperClass(Class)}进行指定.
 * </p>
 * <p>
 * 对于 Mapper 存在两种情况：
 * <ul>
 * <li>若 {@link Reducer} 数为0，这种作业也称为 Map-only 作业。
 * <li>若 {@link Reducer} 数不为0，Mapper 生成键值对输出到 Reducer，这些键值对从 Mapper 输出到 Reducer
 * 的中间过程也称为 shuffle。
 * </ul>
 * Reducer 数通过 {@link JobConf#setNumReduceTasks(int)} 指定，默认 Reducer 数为 Map 数的
 * 1/4，如果是 Map-only 作业，需要显式指定 Reducer 数目为零：job.setNumReduceTasks(0);
 * </p>
 *
 * <p>
 * Mapper 包括三个方法：
 * <ul>
 * <li>{@link #setup(TaskContext)} 在任务开始， map 方法之前调用，每个map调用且仅调用一次。
 * <li>{@link #cleanup(TaskContext)} 在任务结束，map 方法之后调用，每个map调用且仅调用一次。
 * <li>{@link #map(long, Record, TaskContext)} map 方法，每次调用传入一条记录，见 run 方法的默认实现
 * </ul>
 * 上述方法都会传入一个上下文对象，Mapper 实现时可以调用上下文对象的方法，编写代码时最好能多参考下 {@link TaskContext}
 * 及其父类的接口<br/>
 * <br/>
 * 自定义 Mapper 可以重载上述方法。
 *
 * <br/>
 * 对于某些应用，重载 run 方法更合适一些，例如 SleepJob 重载了 run 方法。<br/>
 * <br/>
 * <strong><font color="red"> 注意：传给 map 方法的 Record 对象会被上下文对象复用！</font></strong>
 * <br/>
 * </p>
 *
 * <p>
 * Mapper 可以通过以下几个方法输出计算结果：
 * <ul>
 * <li>{@link TaskContext#write(Record)} 直接输出计算结果到默认输出表
 * <li>{@link TaskContext#write(Record, String)} 直接输出计算结果到指定 label 的输出表
 * <li>{@link TaskContext#write(Record, Record)} 输出中间计算结果（键值对）到 Reducer，此方法要求
 * Reducer 数大于0，否则抛出OperationUnsupportedException。
 * </ul>
 * </p>
 *
 * <p>
 * 键值对通过下面两个方法进行设置。有关Column的定义，见{@link Column}：
 * <ul>
 * <li>{@link JobConf#setMapOutputKeySchema(Column[])}
 * <li>{@link JobConf#setMapOutputValueSchema(Column[])}
 * </ul>
 * </p>
 * <p>
 * 在运行时 Mapper 和 Reducer 可能会起多个任务并发执行，中间结果键值对输出到哪个 Reducer 进行处理由 Partition
 * Columns 决定，用户可以通过 {@link JobConf#setPartitionColumns(String[])}
 * 指定。注意Partition Columns 必须包含在中间结果的键（key）中。
 * </p>
 *
 * <p>
 * 在客户端定义的作业配置（{@link JobConf}）可以通过 {@link TaskContext#getJobConf()} 方法取得。
 * </p>
 *
 * <p>
 * 代码示例，摘自 WordCount:
 *
 * <pre>
 * public static class TokenizerMapper extends MapperBase {
 *
 *   Record word;
 *   Record one;
 *
 *   &#064;Override
 *   public void setup(TaskContext context) throws IOException {
 *     word = context.createMapOutputKeyRecord();
 *     one = context.createMapOutputValueRecord();
 *     one.setBigint(0, 1L);
 *   }
 *
 *   &#064;Override
 *   public void map(long recordNum, Record record, TaskContext context) throws IOException {
 *     for (int i = 0; i &lt; record.getColumnCount(); i++) {
 *       String[] words = record.get(i).toString().split(&quot;\\s+&quot;);
 *       for (String w : words) {
 *         word.setString(0, w); // word.set(new Object[]{w})
 *         context.write(word, one);
 *       }
 *     }
 *   }
 * }
 * </pre>
 *
 * </p>
 *
 * @see TaskContext
 * @see Reducer
 */
public interface Mapper {

  /**
   * 传给{@link Mapper}的上下文对象.
   */
  public interface TaskContext extends com.aliyun.odps.mapred.TaskContext {

    /**
     * 获取当前输入记录序号，记录序号从1开始.
     *
     * @return 当前Map输入记录序号
     */
    public long getCurrentRecordNum();

    /**
     * 获取当前输入记录.
     *
     * 注意：此方法返回的Record对象在{@link #nextRecord()}方法里会被复用
     *
     * @return 当前输入记录
     */
    public Record getCurrentRecord();

    /**
     * 向前读取下一条记录，如果还有未读取的记录，返回true，否则返回false.
     *
     * 读取的记录可以通过{@link #getCurrentRecord()}取得
     *
     * @return 如果还有未读取的记录，返回true，否则返回false
     */
    public boolean nextRecord();

    /**
     * 获取Map的输入表或分区的信息.
     *
     * <p>
     * ODPS M/R 程序支持多路输入，输入可以是表或分区，本方法返回的是当前 {@link Mapper} 所负责处理的表或分区的信息.
     * </p>
     * <p>
     * 例如，输入是一个分区表 t，该表有两级四个分区：
     * <ul>
     * <li>pt=1/ds=1
     * <li>pt=1/ds=2
     * <li>pt=2/ds=1
     * <li>pt=2/ds=2
     * </ul>
     * </p>
     * 如果每个分区都有数据，即使数据量非常小，也至少会划分成 4 个 {@link Mapper} 来跑，每个 {@link Mapper}
     * 可以通过本方法获取当前分区的信息（{@link TableInfo}）
     */
    public TableInfo getInputTableInfo();
  }

  /**
   * 在任务开始， map 方法之前调用.
   *
   * @param context
   *     Map 上下文对象
   * @throws IOException
   */
  void setup(TaskContext context) throws IOException;

  /**
   * map 方法，处理输入表的记录.
   *
   * <p>
   * <strong><font color="red"> 注意：传给 map 方法的 Record 对象会被上下文对象复用，
   * 即每次map方法调用都传入相同的Record对象。若需要保存上一条记录的数据，使用{@link Record#toArray()}
   * </font></strong>
   *
   * @param key
   *     当前的记录序号
   * @param record
   *     当前记录对象
   * @param context
   *     Map 上下文对象
   * @throws IOException
   */
  void map(long key, Record record, TaskContext context) throws IOException;

  /**
   * 在任务结束，map 方法之后调用.
   *
   * @param context
   *     Map 上下文对象
   * @throws IOException
   */
  void cleanup(TaskContext context) throws IOException;
}
