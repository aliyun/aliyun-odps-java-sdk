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

package com.aliyun.odps.mapred.bridge;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.TableInfo.TableInfoBuilder;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.data.RecordComparator;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.bridge.type.ColumnBasedRecordComparator;
import com.aliyun.odps.mapred.bridge.utils.MapReduceUtils;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.annotation.NotReuseArgumentObject;
import com.aliyun.odps.udf.annotation.PreferWritable;
import com.aliyun.odps.utils.ReflectionUtils;

@PreferWritable
@NotReuseArgumentObject
public class LotMapperUDTF extends LotTaskUDTF {

  class DirectMapContextImpl extends UDTFTaskContextImpl implements TaskContext {

    Record record;
    TableInfo inputTableInfo;
    Partitioner partitioner;

    public DirectMapContextImpl(BridgeJobConf conf, TableInfo tableInfo, ExecutionContext context) {
      super(conf);
      inputTableInfo = tableInfo;
      record = new WritableRecord(inputSchema);
      // only map stage need column access info now
      ((WritableRecord)record).setEnableColumnAccessStat(true);
      configure(context);
    }

    @Override
    boolean isMapper() {
      return true;
    }

    @SuppressWarnings({"unchecked"})
    public void configure(ExecutionContext ctx) {
      super.configure(ctx);
      Class<? extends Partitioner> partitionerClass;
      if (pipeMode) {
        conf.setMapperClass(pipeNode.getTransformClass());
        partitionerClass = pipeNode.getPartitionerClass();
      } else {
        partitionerClass = getJobConf().getPartitionerClass();
      }

      // support for different mapper-class of multiple input tables
      if (inputTableInfo != null && inputTableInfo.getMapperClass() != null) {
          conf.setMapperClass((Class<? extends Mapper>) (inputTableInfo.getMapperClass()));
      }

      if (partitionerClass != null) {
        partitioner = ReflectionUtils.newInstance(partitionerClass, getJobConf());
        partitioner.configure(conf);
      }

      // for inner output
      if (innerOutput && reducerNum > 0) {
        Column[] keyCols = new Column[0];
        Column[] valCols = new Column[0];
        if (pipeMode && pipeNode != null && pipeNode.getType().equals("map")) {
          keyCols = pipeNode.getOutputKeySchema();
          valCols = pipeNode.getOutputValueSchema();
        } else {
          keyCols = conf.getMapOutputKeySchema();
          valCols = conf.getMapOutputValueSchema();
        }

        Column[] outputFields = new Column[keyCols.length + valCols.length + packagedOutputSchema.length];
        int len = 0;
        for (Column col : keyCols) {
          outputFields[len++] = col;
        }
        for (Column col : valCols) {
          outputFields[len++] = col;
        }
        innerOutputIndex = len;
        for (Column col : packagedOutputSchema) {
          outputFields[len++] = col;
        }
        packagedOutputSchema = outputFields;
      }
    }

    @Override
    public void write(Record r) throws IOException {
      write(r, TableInfo.DEFAULT_LABEL);
    }

    @Override
    public void write(Record r, String label) throws IOException {
      if (!innerOutput && getNumReduceTasks() > 0) {
        throw new UnsupportedOperationException(ErrorCode.UNEXPECTED_MAP_WRITE_OUTPUT.toString());
      }
      if (!hasLabel(label)) {
        throw new IOException(ErrorCode.NO_SUCH_LABEL.toString() + " " + label);
      }
      if (innerOutput) {
        write(createInnerOutputRow(((WritableRecord)r).toWritableArray(), true, TableInfo.INNER_OUTPUT_LABEL, label));
      } else {
        write(createOutputRow(r, label));
      }
    }

    @Override
    public void write(Record key, Record value) {
      if (getNumReduceTasks() == 0) {
        throw new UnsupportedOperationException(ErrorCode.UNEXPECTED_MAP_WRITE_INTER.toString());
      }
      Writable[] keyArray = ((WritableRecord) key).toWritableArray();
      Writable[] valueArray = ((WritableRecord) value).toWritableArray();
      Writable[] result;
      int idx = 0;
      if (partitioner != null) {
        int part = partitioner.getPartition(key, value, getNumReduceTasks());
        if (part < 0 || part >= getNumReduceTasks()) {
          throw new RuntimeException("partitioner return invalid partition value:" + part);
        }
        result = new Writable[1 + keyArray.length + valueArray.length];
        result[idx++] = new LongWritable(part);
      } else {
        result = new Writable[keyArray.length + valueArray.length];
      }
      for (Writable obj : keyArray) {
        result[idx++] = obj;
      }
      for (Writable obj : valueArray) {
        result[idx++] = obj;
      }
      if (innerOutput) {
        write(createInnerOutputRow(result, false, TableInfo.DEFAULT_LABEL, TableInfo.DEFAULT_LABEL));
      } else {
        write(result);
      }
    }

    protected void write(Object[] record) {
      collect(record);
    }

    @Override
    public long getCurrentRecordNum() {
      return rowNum;
    }

    @Override
    public Record getCurrentRecord() {
      return record;
    }

    @Override
    public boolean nextRecord() {
      Object[] values = getNextRowWapper();
      if (values == null) {
        return false;
      }
      rowNum++;
      if (rowNum == nextCntr) {
        StateInfo.updateMemStat();
        nextCntr = getNextCntr(rowNum, true);
      }
      if (rowNum == nextRecordCntr) {
        StateInfo.updateMemStat("processed " + rowNum + " records");
        nextRecordCntr = getNextCntr(rowNum, false);
      }
      record.set(values);
      return true;
    }

    @Override
    public TableInfo getInputTableInfo() {
      return inputTableInfo;
    }

    void AddFrameworkCounters() {
      // not to break counters number limit
      try {
        ctx.getCounter("ODPS_SDK_FRAMEWORK_COUNTER_GROUP", "input_col_total_num").setValue(record.getColumnCount());
        ctx.getCounter("ODPS_SDK_FRAMEWORK_COUNTER_GROUP", "input_col_used_num").setValue(((WritableRecord) record).getColumnAccessedNum());
      } catch (IllegalArgumentException e) {
        // ignore it
      }
    }

  }

  interface CombineContext {
    public void offerKeyValue(Record key, Record value) throws InterruptedException;
    public void spill();
  }

  /**
   * A proxied map context implementation.
   */
  class ProxiedMapContextImpl extends DirectMapContextImpl implements Closeable {


    class NonGroupingCombineContextImpl extends DirectMapContextImpl implements
                                                         com.aliyun.odps.mapred.Reducer.TaskContext, CombineContext {
      private class CombinerBuffer extends TreeMap<Object[], List<Object[]>> {

        private static final long serialVersionUID = 1L;

        public CombinerBuffer(Comparator<Object[]> comparator) {
          super(comparator);
        }

        public boolean offerKeyValue(Record key, Record value) throws InterruptedException {
          boolean rt = false;
          Object[] objKey = Arrays.copyOf(((WritableRecord) key).toWritableArray(), key.getColumnCount());
          List<Object[]> objValue;
          if (super.containsKey(objKey)) {
            objValue = super.get(objKey);
          } else {
            objValue = new ArrayList<Object[]>();
            super.put(objKey, objValue);
          }
          rt = objValue.add(Arrays.copyOf(((WritableRecord) value).toWritableArray(), value.getColumnCount()));

          return rt;
        }
      }

      private Record key;
      private Record value;
      private Iterator<Entry<Object[], List<Object[]>>> itr;
      private List<Object[]> curValuelist;
      
      private int velocity = 0;
      private int threshold = 0;
      private int size = 0;

      private CombinerBuffer combinerBuffer;
      private CombinerBuffer backupCombinerBuffer;
      private NonGroupingRecordIterator recordsItr;

      public NonGroupingCombineContextImpl(BridgeJobConf conf, int velocity, float spillPercent,
          Comparator<Object[]> keyComparator, ExecutionContext context) {
        super(conf, null, context);

        if (pipeMode) {
          key = new WritableRecord(pipeNode.getOutputKeySchema());
          value = new WritableRecord(pipeNode.getOutputValueSchema());
        } else {
          key = new WritableRecord(conf.getMapOutputKeySchema());
          value = new WritableRecord(conf.getMapOutputValueSchema());
        }

        this.velocity = velocity;
        threshold = (int) (velocity * spillPercent);
        if (threshold < 0) {
          threshold = 0;
        } else if (threshold > velocity) {
          threshold = velocity;
        }

        combinerBuffer = new CombinerBuffer(keyComparator);
        backupCombinerBuffer = new CombinerBuffer(keyComparator);
      }

      @Override
      public void write(Record r) throws IOException {
        ProxiedMapContextImpl.this.write(r);
      }

      @Override
      public void write(Record r, String label) throws IOException {
        ProxiedMapContextImpl.this.write(r, label);
      }

      @Override
      public Record getCurrentKey() {
        return key;
      }

      @Override
      public boolean nextKeyValue() {
        if (itr.hasNext()) {
          Entry<Object[], List<Object[]>> entry = itr.next();
          curValuelist = entry.getValue();
          recordsItr = new NonGroupingRecordIterator(curValuelist, (WritableRecord) value);
          key.set(Arrays.copyOf(entry.getKey(), key.getColumnCount()));
          return true;
        }
        
        return false;
      }

      @Override
      public Iterator<Record> getValues() {
        return recordsItr;
      }

      @Override
      public void write(Record key, Record value) {
        try {
          backupCombinerBuffer.offerKeyValue(key, value);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      
      @Override
      public void offerKeyValue(Record key, Record value) throws InterruptedException {
        combinerBuffer.offerKeyValue(key, value);
        
        if (++size >= velocity) {
          combine();
          // after combine, check backupCombinerBuffer's size, whether need to spill
          if (backupCombinerBuffer.size() >= threshold) {
            spill();
          } else {
            // just swap the two buffers
            CombinerBuffer tmp = null;
            tmp = combinerBuffer;
            combinerBuffer = backupCombinerBuffer;
            backupCombinerBuffer = tmp;
            size = combinerBuffer.size();
          }
        }
      }

      @Override
      public void spill() {
        if (combinerBuffer.size() > 0) {
          combine();
        }
        for (Entry<Object[], List<Object[]>> it : backupCombinerBuffer.entrySet()) {
          List<Object[]> objList = it.getValue();
          ((WritableRecord)key).set(it.getKey());
          ((WritableRecord)value).set(objList.get(0));
          ProxiedMapContextImpl.this.writeDirect(key, value);
        }
        backupCombinerBuffer.clear();
      }
      

      private void combine() {
        this.itr = combinerBuffer.entrySet().iterator();

        try {
          MapReduceUtils.runReducer((Class<Reducer>) this.getCombinerClass(), this);
        } catch (IOException e) {
          throw new RuntimeException(e);
        } finally {
          combinerBuffer.clear();
          size = 0;
        }
      }
    }

    class GroupingCombineContextImpl extends DirectMapContextImpl implements com.aliyun.odps.mapred.Reducer.TaskContext, CombineContext {
      private class CombinerBuffer extends PriorityQueue<Object[]> {

        private static final long serialVersionUID = 1L;

        public CombinerBuffer(int velocity, Comparator<Object[]> comparator) {
          super(velocity, comparator);
        }

        public boolean offerKeyValue(Record key, Record value) throws InterruptedException {
          return super.offer(ArrayUtils.addAll(((WritableRecord) key).toWritableArray(),
                                                     ((WritableRecord) value).toWritableArray()));
        }

      }
      private int velocity = 0;
      private Record key, value;
      private GroupingRecordIterator itr;
      private CombinerBuffer combinerBuffer;
      private Comparator<Object[]> keyComparator = null;
      private Comparator<Object[]> keyGroupingComparator = null;

      public GroupingCombineContextImpl(BridgeJobConf conf, int bufferSize,
          Comparator<Object[]> comparator, ExecutionContext context) {
        super(conf, null, context);
        configure(context, comparator);
        velocity = bufferSize;
        combinerBuffer = new CombinerBuffer(bufferSize, keyComparator);
      }

      public void configure(ExecutionContext ctx, Comparator<Object[]> comparator) {
        super.configure(ctx);

        keyComparator = comparator;

        String[] keyGrpColumns;
        Column[] keyRS;
        JobConf.SortOrder[] keySortOrder;
        Class< ? extends RecordComparator> keyComparatorClass = null;
        Class< ? extends RecordComparator> keyGroupingComparatorClass = null;
        if (pipeMode) {
          key = new WritableRecord(pipeNode.getOutputKeySchema());
          value = new WritableRecord(pipeNode.getOutputValueSchema());
          keyGrpColumns = pipeNode.getOutputGroupingColumns();
          keyRS = pipeNode.getOutputKeySchema();
          keySortOrder = pipeNode.getOutputKeySortOrder();
          keyComparatorClass = pipeNode.getInputKeyComparatorClass();
          keyGroupingComparatorClass = pipeNode.getInputKeyGroupingComparatorClass();
          
        } else {
          key = new WritableRecord(conf.getMapOutputKeySchema());
          value = new WritableRecord(conf.getMapOutputValueSchema());
          keyGrpColumns = conf.getOutputGroupingColumns();
          keyRS = conf.getMapOutputKeySchema();
          keySortOrder = conf.getOutputKeySortOrder();
          keyComparatorClass = conf.getOutputKeyComparatorClass();
          keyGroupingComparatorClass = conf.getOutputKeyGroupingComparatorClass();
        }

        if (keyGroupingComparatorClass != null) {
          keyGroupingComparator = ReflectionUtils.newInstance(keyGroupingComparatorClass, getJobConf());
        } else if (keyComparatorClass != null) {
          keyGroupingComparator = keyComparator;
        } else {
          keyGroupingComparator = new ColumnBasedRecordComparator(keyGrpColumns, keyRS, keySortOrder);
        }
      }

      @Override
      public void write(Record r) throws IOException {
        ProxiedMapContextImpl.this.write(r);
      }

      @Override
      public void write(Record r, String label) throws IOException {
        ProxiedMapContextImpl.this.write(r, label);
      }

      @Override
      public Record getCurrentKey() {
        return key;
      }

      @Override
      public boolean nextKeyValue() {
        if (itr == null) {
          Object[] init = combinerBuffer.peek();
          if (init == null) {
            return false;
          }
          itr = new GroupingRecordIterator(combinerBuffer, (WritableRecord) key,
                    (WritableRecord) value, keyGroupingComparator);
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
      public Iterator<Record> getValues() {
        return itr;
      }

      @Override
      public void write(Record key, Record value) {
        ProxiedMapContextImpl.this.writeDirect(key, value);
      }

      @Override
      public void spill() {
        combine();
      }

      @Override
      public void offerKeyValue(Record key, Record value) throws InterruptedException {
        combinerBuffer.offerKeyValue(key, value);
        if (combinerBuffer.size() >= velocity) {
          combine();
        }
      }

      private void combine() {
        itr = null;
        try {
          MapReduceUtils.runReducer((Class<Reducer>) this.getCombinerClass(), this);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    CombineContext combineCtx;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public ProxiedMapContextImpl(BridgeJobConf conf, TableInfo tableInfo,
                                 ExecutionContext context) {
      super(conf, tableInfo, context);

      int bufferSize = conf.getCombinerCacheItems();
      float combineBufferSpillPercent = conf.getCombinerCacheSpillPercent();
      Comparator<Object[]> keyComparator;
      Column[] keyRS;
      String[] keySortColumns;
      String[] keyGrpColumns;
      JobConf.SortOrder[] keySortOrder;
      Class< ? extends RecordComparator> keyComparatorClass = null;
      Class< ? extends RecordComparator> keyGroupingComparatorClass = null;
      if (pipeMode) {
        keyRS = pipeNode.getOutputKeySchema();
        keySortColumns = pipeNode.getOutputKeySortColumns();
        keyGrpColumns = pipeNode.getOutputGroupingColumns();
        keySortOrder = pipeNode.getOutputKeySortOrder();
        // hotfix for sprint21, should remove in sprint23
//        keyComparatorClass = pipeNode.getOutputKeyComparatorClass();
//        keyGroupingComparatorClass = pipeNode.getOutputKeyGroupingComparatorClass();
        keyComparatorClass = conf.getPipelineOutputKeyComparatorClass(pipeIndex);
        keyGroupingComparatorClass = conf.getPipelineOutputKeyGroupingComparatorClass(pipeIndex);
      } else {
        keyRS = conf.getMapOutputKeySchema();
        keySortColumns = conf.getOutputKeySortColumns();
        keyGrpColumns = conf.getOutputGroupingColumns();
        keySortOrder = conf.getOutputKeySortOrder();
        keyComparatorClass = conf.getOutputKeyComparatorClass();
        keyGroupingComparatorClass = conf.getOutputKeyGroupingComparatorClass();
      }
      
      if (keyComparatorClass != null) {
        keyComparator = ReflectionUtils.newInstance(keyComparatorClass, getJobConf());
      } else {
        keyComparator = new ColumnBasedRecordComparator(keySortColumns, keyRS, keySortOrder);
      }

      if (conf.getCombinerOptimizeEnable() && Arrays.deepEquals(keySortColumns, keyGrpColumns)
          && keyComparatorClass == keyGroupingComparatorClass) {
        combineCtx =
            new NonGroupingCombineContextImpl(conf, bufferSize, combineBufferSpillPercent,
                keyComparator, context);
      } else {
        combineCtx = new GroupingCombineContextImpl(conf, bufferSize, keyComparator, context);
      }
    }

    @Override
    public void write(Record key, Record value) {
      try {
        combineCtx.offerKeyValue(key, value);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void writeDirect(Record key, Record value) {
      super.write(key, value);
    }

    @Override
    public void close() throws IOException {
      combineCtx.spill();
    }
  }

  private TaskContext ctx;
  private long rowNum;
  private long nextCntr = 1;
  private long nextRecordCntr = 1;
  private Column[] inputSchema;

  TableInfo getTableInfoFromDesc(String inputSpec) {
    if (inputSpec == null || inputSpec.isEmpty()) {
      throw new RuntimeException(ErrorCode.INTERNAL_ERROR.toString()
                                 + ": Input table spec not found");
    }
    if (inputSpec.indexOf(";") > 0) {
      // Not supporting extreme storage, we simply skipping the partition spec
      // in that case.
      int sepIdx = inputSpec.indexOf(";");
      int divIdx = inputSpec.indexOf("/");
      divIdx = divIdx < 0 ? Integer.MAX_VALUE : divIdx;
      sepIdx = Math.min(sepIdx, divIdx);
      inputSpec = inputSpec.substring(0, sepIdx);
    }
    TableInfoBuilder builder = TableInfo.builder();
    String[] kv = inputSpec.split("/", 2);
    String tableSpec = kv[0];
    String[] prjtbl = tableSpec.split("\\.", 2);
    builder.projectName(prjtbl[0]).tableName(prjtbl[1]);
    if (kv.length == 2) {
      builder.partSpec(kv[1]);
    }
    return builder.build();
  }

  TableInfo getTableInfo(TableInfo[] inputs, String inputSpec) {
    TableInfo info = getTableInfoFromDesc(inputSpec);

    // Set additional info
    for (TableInfo input : inputs) {
      if (MapReduceUtils.partSpecInclusive(input, info)) {
        info.setCols(input.getCols());
        info.setLable(input.getLabel());
      }
    }
    return info;
  }

  @Override
  public void setup(ExecutionContext context) {
    TableInfo tableInfo = null;
    TableInfo[] inputs = InputUtils.getTables(conf);

    if (inputs != null && inputs.length > 0) {
      String inputSpec = context.getTableInfo();
      tableInfo = getTableInfo(inputs, inputSpec);
    }
    if (tableInfo != null) {
      inputSchema = conf.getInputSchema(tableInfo);
    } else {
      inputSchema = new Column[]{};
    }

    if (conf.getCombinerClass() != null && conf.getNumReduceTasks() > 0) {
      ctx = new ProxiedMapContextImpl(conf, tableInfo, context);
    } else {
      ctx = new DirectMapContextImpl(conf, tableInfo, context);
    }

  }

  @Override
  public void close() {
    try {
      if (ctx instanceof ProxiedMapContextImpl) {
        ((ProxiedMapContextImpl) ctx).close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void run() throws IOException {
    StateInfo.init();
    MapReduceUtils.runMapper((Class<Mapper>) conf.getMapperClass(), ctx);
    // add column access info counters here, now all the user code finished
    if (ctx instanceof DirectMapContextImpl) {
      ((DirectMapContextImpl) ctx).AddFrameworkCounters();
    }
    StateInfo.updateMemStat("mapper end");
    StateInfo.printMaxMemo();
  }

  public Object[] getNextRowWapper() {
    Object[] data = getNextRow();
    if (data != null) {
      return data.clone();
    } else {
      return null;
    }
  }

}
