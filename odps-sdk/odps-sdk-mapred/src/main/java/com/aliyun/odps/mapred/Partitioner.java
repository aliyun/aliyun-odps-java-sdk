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

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.conf.JobConf;

/**
 * 分区类，决定 {@link Mapper} 写出的中间结果（键值对）输出给哪个 {@link Reducer} .
 *
 * 可以通过{@link JobConf#setPartitionerClass(Class)}方法自定义分区类，否则会使用分区列：
 * {@link JobConf#setPartitionColumns(String[])}
 *
 * 注意：当用户同时指定{@link JobConf#setPartitionerClass(Class)}和{@link JobConf#setPartitionColumns(String[])}时，用户行为未定义。
 *
 * @see Mapper
 * @see Reducer
 */
public abstract class Partitioner {

  /**
   * 此方法会在分区类对象被new出来后立即调用，传入运行时的{@link JobConf}对象
   *
   * @param job
   *     运行时的JobConf对象
   */
  public void configure(JobConf job) {
  }

  ;

  /**
   * 给定Key/Value和分区数（numPartitions），计算该Key/Value输出到哪个分区（分区索引从0到numPartitions-1），
   * 这里分区数（numPartitions）就是Reduce个数
   *
   * <p>
   * 此方法通常实现为对key的某个哈希函数，可以参考
   * {@link HashPartitioner#getPartition(Object, Object, int)}的实现
   * </p>
   *
   * @param key
   *     待分区的Key
   * @param value
   *     待分区的Value
   * @param numPartitions
   *     分区数，既Reduce个数
   * @return 范围从0到numPartitions-1的分区索引
   */
  public abstract int getPartition(Record key, Record value, int numPartitions);

}
