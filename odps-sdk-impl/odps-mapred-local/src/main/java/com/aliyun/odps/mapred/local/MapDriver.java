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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.local.common.FileSplit;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.utils.PartitionUtils;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.bridge.ErrorCode;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.aliyun.odps.mapred.bridge.type.ColumnBasedRecordComparator;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.utils.ReflectionUtils;
import com.aliyun.odps.volume.FileSystem;

public class MapDriver extends DriverBase {

  public static final Log LOG = LogFactory.getLog(MapDriver.class);

  private TaskContext mapContext;
  MapOutputBuffer outputBuffer;
  private Counters counters;

  class DirectMapContextImpl extends LocalTaskContext implements TaskContext {

    int rowNumber = 1;
    protected RecordReader reader;
    Record record;
    protected Counter mapOutputRecordCounter;
    protected TableInfo inputTableInfo;
    protected Partitioner partitioner;

    public DirectMapContextImpl(BridgeJobConf conf, TaskId taskid, Counters counters,
                                RecordReader reader, TableInfo inputTableInfo) throws IOException {
      super(conf, taskid, counters);
      this.reader = reader;
      mapOutputRecordCounter = counters.findCounter(JobCounter.MAP_OUTPUT_RECORDS);
      this.inputTableInfo = inputTableInfo;

      Class<? extends Partitioner> partitionerClass;
      if (pipeMode) {
        conf.setMapperClass(pipeNode.getTransformClass());
        partitionerClass = pipeNode.getPartitionerClass();
      } else {
        partitionerClass = getJobConf().getPartitionerClass();
      }

      if (partitionerClass != null) {
        partitioner = ReflectionUtils.newInstance(partitionerClass, getJobConf());
        partitioner.configure(conf);
      }
    }

    @Override
    public long getCurrentRecordNum() {
      return rowNumber;
    }

    @Override
    public Record getCurrentRecord() {
      return record;
    }

    @Override
    public boolean nextRecord() {
      try {
        record = reader.read();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (record == null) {
        return false;
      }
      return true;
    }

    @Override
    public void write(Record record) throws IOException {
      mapOutputRecordCounter.increment(1);
      write(record, TableInfo.DEFAULT_LABEL);
    }

    @Override
    public void write(Record record, String label) throws IOException {
      if (getNumReduceTasks() > 0) {
        throw new UnsupportedOperationException(ErrorCode.UNEXPECTED_MAP_WRITE_OUTPUT.toString());
      }
      if (outputBuffer != null) {
        outputBuffer.add(record, label);
      }
      recordWriters.get(label).write(record);
      counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1);
    }

    @Override
    public void write(Record key, Record value) {
      if (getNumReduceTasks() == 0) {
        throw new UnsupportedOperationException(ErrorCode.UNEXPECTED_MAP_WRITE_INTER.toString());
      }
      mapOutputRecordCounter.increment(1);

      if (partitioner != null) {
        int part = partitioner.getPartition(key, value, getNumReduceTasks());
        if (part < 0 || part >= getNumReduceTasks()) {
          throw new RuntimeException("partitioner return invalid partition value:" + part);
        }
        outputBuffer.add(key, value, part);
      } else {
        outputBuffer.add(key, value);
      }

      counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1);
    }

    public void close() throws IOException {
      reader.close();
      closeWriters();
    }

    @Override
    public TableInfo getInputTableInfo() {
      return inputTableInfo;
    }

    @Override
    public Record createOutputKeyRecord() throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Record createOutputValueRecord() throws IOException {
      // TODO Auto-generated method stub
      return null;
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

  class ProxiedMapContextImpl extends DirectMapContextImpl implements TaskContext {

    class CombinerContextImpl extends DirectMapContextImpl implements
                                                       com.aliyun.odps.mapred.Reducer.TaskContext {

      private Record key;
      private Iterator<Record> itr;
      private Counter combineInputGroupCounter;
      private Counter combineOutputRecordCounter;

      public CombinerContextImpl(BridgeJobConf conf, TaskId taskid, Counters counters)
          throws IOException {
        super(conf, taskid, counters, null, null);
        combineInputGroupCounter = counters.findCounter(JobCounter.COMBINE_INPUT_GROUPS);
        combineOutputRecordCounter = counters.findCounter(JobCounter.COMBINE_OUTPUT_RECORDS);
      }

      @Override
      public boolean nextKeyValue() {
        if (itr == null) {
          Object[] init = queue.peek();
          if (init == null) {
            return false;
          }
          key = createMapOutputKeyRecord();
          Record value = createMapOutputValueRecord();
          String[] groupingColumns = getGroupingColumns();
          Comparator<Object[]> grpComparator = new ColumnBasedRecordComparator(
              groupingColumns, key.getColumns());
          itr =
              new LocalGroupingRecordIterator(queue, (WritableRecord) key, (WritableRecord) value,
                                              grpComparator, false, counters);
          key.set(Arrays.copyOf(init, key.getColumnCount()));
        } else {
          while (itr.hasNext()) {
            itr.remove();
          }
          if (!((LocalGroupingRecordIterator) itr).reset()) {
            return false;
          }
        }
        combineInputGroupCounter.increment(1);
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
      public void write(Record record) throws IOException {
        write(record, TableInfo.DEFAULT_LABEL);
        combineOutputRecordCounter.increment(1);
      }

      @Override
      public void write(Record record, String label) throws IOException {
        recordWriters.get(label).write(record);
      }

      @Override
      public void write(Record key, Record value) {
        if (partitioner != null) {
          int part = partitioner.getPartition(key, value, this.getNumReduceTasks());
          if (part < 0 || part >= this.getNumReduceTasks()) {
            throw new RuntimeException("partitioner return invalid partition value:" + part);
          }
          outputBuffer.add(key, value, part);
        } else {
          outputBuffer.add(key, value);
        }
        combineOutputRecordCounter.increment(1);
      }

      @Override
      public Record createOutputKeyRecord() throws IOException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Record createOutputValueRecord() throws IOException {
        // TODO Auto-generated method stub
        return null;
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

    private LinkedList<Object[]> queue = new LinkedList<Object[]>();

    public ProxiedMapContextImpl(BridgeJobConf conf, TaskId taskid, Counters counters,
                                 RecordReader reader, TableInfo inputTableInfo) throws IOException {
      super(conf, taskid, counters, reader, inputTableInfo);
    }

    @Override
    public void write(Record key, Record value) {
      mapOutputRecordCounter.increment(1);
      queue.add(ArrayUtils.addAll(((WritableRecord) key).toWritableArray(),
                                  ((WritableRecord) value).toWritableArray()));
      counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).increment(1);
    }

    @Override
    public void close() throws IOException {
      Collections.sort(queue, outputBuffer.getComparator());
      Reducer combiner = ReflectionUtils.newInstance(getCombinerClass(), conf);
      CombinerContextImpl combineCtx = new CombinerContextImpl(conf, taskId, counters);

      LOG.info("Start to run Combiner, TaskId: " + taskId);

      combiner.setup(combineCtx);
      while (combineCtx.nextKeyValue()) {
        combiner.reduce(combineCtx.getCurrentKey(), combineCtx.getValues(), combineCtx);
      }
      combiner.cleanup(combineCtx);

      super.close();

      LOG.info("Fininshed run Combiner, TaskId: " + taskId);
    }
  }

  public MapDriver(BridgeJobConf job, FileSplit split, TaskId id, final MapOutputBuffer buffer,
                   final Counters counters, TableInfo tableInfo) throws IOException {

    super(job, id, counters);
    this.outputBuffer = buffer;
    this.split = split;

    Counter mapInputByteCounter = counters
        .findCounter(JobCounter.MAP_INPUT_BYTES);
    Counter mapInputRecordCounter = counters
        .findCounter(JobCounter.MAP_INPUT_RECORDS);
    TableMeta tableMeta = null;
    if (tableInfo != null) {
      tableMeta = new TableMeta(tableInfo.getProjectName(), tableInfo.getTableName(), null);
    }
    RecordReader reader =
        new CSVRecordReader(split, tableMeta, mapInputRecordCounter, mapInputByteCounter, counters,
            WareHouse.getInstance().getInputColumnSeperator());
    if (job.getCombinerClass() != null) {
      mapContext = new ProxiedMapContextImpl(job, taskId, counters, reader, tableInfo);
    } else {
      mapContext = new DirectMapContextImpl(job, id, counters, reader, tableInfo);
    }

    this.counters = counters;
  }

  private FileSplit split;

  public void run() throws IOException {

    Mapper mapper = ((LocalTaskContext) mapContext).createMapper();

    // Fill Partition Info
    if (split != null && split != FileSplit.NullSplit) {
      File whInputFile = WareHouse.getInstance().getJobDir().convertInputFile(split.getFile());
      PartitionSpec partitionSpec = WareHouse.getInstance().resolvePartition(
          mapContext.getInputTableInfo().getProjectName(),
          mapContext.getInputTableInfo().getTableName(), whInputFile);
      mapContext.getInputTableInfo().setPartSpec(PartitionUtils.convert(partitionSpec));
    }

    mapper.setup(mapContext);
    while (mapContext.nextRecord()) {
      mapper.map(mapContext.getCurrentRecordNum(), mapContext.getCurrentRecord(), mapContext);
    }
    mapper.cleanup(mapContext);

    ((DirectMapContextImpl) mapContext).close();

  }

}
