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

package com.aliyun.odps.mapred.local;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.bridge.ErrorCode;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.aliyun.odps.mapred.bridge.type.ColumnBasedRecordComparator;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.utils.ReflectionUtils;
import com.aliyun.odps.volume.FileSystem;

public class ReduceDriver extends DriverBase {

  class ReduceContextImpl extends LocalTaskContext implements TaskContext, Closeable {

    private Record key;
    private Comparator<Object[]> keyGroupingComparator;
    private LocalGroupingRecordIterator itr;
    private Queue<Object[]> queue;
    private MapOutputBuffer outputBuffer;
    private Partitioner partitioner;
    private Counters counters;

    public ReduceContextImpl(BridgeJobConf job, TaskId taskId, Counters counters,
                             Queue<Object[]> queue, MapOutputBuffer outputBuffer)
        throws IOException {
      super(job, taskId, counters);
      Class<? extends Partitioner> partitionerClass = null;
      if (pipeMode) {
        conf.setReducerClass(pipeNode.getTransformClass());
        key = new WritableRecord(pipeNode.getInputKeySchema());
        keyGroupingComparator = new ColumnBasedRecordComparator(pipeNode.getInputGroupingColumns(),
                                                                key.getColumns());
        partitionerClass = pipeNode.getPartitionerClass();
      } else {
        key = new WritableRecord(conf.getMapOutputKeySchema());
        keyGroupingComparator = new ColumnBasedRecordComparator(conf.getOutputGroupingColumns(),
                                                                key.getColumns());
      }

      if (partitionerClass != null) {
        partitioner = ReflectionUtils.newInstance(partitionerClass, getJobConf());
        partitioner.configure(conf);
      }

      this.queue = queue;
      this.outputBuffer = outputBuffer;
      this.counters = counters;
    }

    @Override
    public void write(Record record) throws IOException {
      write(record, TableInfo.DEFAULT_LABEL);
    }

    @Override
    public void write(Record record, String label) throws IOException {
      if (outputBuffer != null) {
        outputBuffer.add(record, label);
      }
      recordWriters.get(label).write(record);
      counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1);
    }

    @Override
    public boolean nextKeyValue() {
      if (itr == null) {
        Object[] init = queue.peek();
        if (init == null) {
          return false;
        }
        Record value;
        if (pipeMode) {
          value = new WritableRecord(pipeNode.getInputValueSchema());
        } else {
          value = new WritableRecord(conf.getMapOutputValueSchema());
        }
        itr =
            new LocalGroupingRecordIterator(queue, (WritableRecord) key, (WritableRecord) value,
                                            keyGroupingComparator, true, counters);
        key.set(Arrays.copyOf(init, key.getColumnCount()));
      } else {
        while (itr.hasNext()) {
          itr.remove();
        }
        if (!itr.reset()) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Record getCurrentKey() {
      return key;
    }

    @Override
    public Iterator<Record> getValues() {
      return itr;
    }

    @Override
    public void write(Record key, Record value) {
      if (!pipeMode || pipeNode == null) {
        throw new UnsupportedOperationException(
            ErrorCode.INTERMEDIATE_OUTPUT_IN_REDUCER.toString());
      }

      if (partitioner != null) {
        int part = partitioner.getPartition(key, value, this.getNumReduceTasks());
        if (part < 0 || part >= this.getNumReduceTasks()) {
          throw new RuntimeException("partitioner return invalid partition value:" + part);
        }
        this.outputBuffer.add(key, value, part);
      } else {
        this.outputBuffer.add(key, value);
      }

      counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1);
    }

    public void close() throws IOException {
      closeWriters();
    }

    @Override
    public VolumeInfo getInputVolumeInfo() throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public VolumeInfo getInputVolumeInfo(String label) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public VolumeInfo getOutputVolumeInfo() throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public VolumeInfo getOutputVolumeInfo(String label) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FileSystem getInputVolumeFileSystem() throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FileSystem getInputVolumeFileSystem(String label) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FileSystem getOutputVolumeFileSystem() throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FileSystem getOutputVolumeFileSystem(String label) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }
  }


  public static final Log LOG = LogFactory.getLog(ReduceDriver.class);
  private final TaskContext reduceContext;
  MapOutputBuffer inputBuffer;
  MapOutputBuffer outputBuffer;

  public ReduceDriver(BridgeJobConf job, MapOutputBuffer inputBuffer, MapOutputBuffer outputBuffer,
                      TaskId id, Counters counters, int partitionIndex) throws IOException {
    super(job, id, counters);
    this.inputBuffer = inputBuffer;
    this.outputBuffer = outputBuffer;
    this.reduceContext = new ReduceContextImpl(job, taskId, counters,
                                               inputBuffer.getPartitionQueue(partitionIndex),
                                               outputBuffer);
  }

  public void run() throws IOException {

    Reducer reducer = ((LocalTaskContext) reduceContext).createReducer();

    reducer.setup(reduceContext);
    while (reduceContext.nextKeyValue()) {
      reducer.reduce(reduceContext.getCurrentKey(), reduceContext.getValues(), reduceContext);
    }
    reducer.cleanup(reduceContext);

    ((ReduceContextImpl) reduceContext).close();

  }
}
