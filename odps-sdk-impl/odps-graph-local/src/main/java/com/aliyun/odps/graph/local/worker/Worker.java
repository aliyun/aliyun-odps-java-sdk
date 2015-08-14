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

package com.aliyun.odps.graph.local.worker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.Aggregator;
import com.aliyun.odps.graph.Combiner;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.JobConf;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.VertexResolver;
import com.aliyun.odps.graph.WorkerComputer;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.graph.local.BaseRecordReader;
import com.aliyun.odps.graph.local.COUNTER;
import com.aliyun.odps.graph.local.EmptyRecordReader;
import com.aliyun.odps.graph.local.GraphTaskAttemptID;
import com.aliyun.odps.graph.local.InputSplit;
import com.aliyun.odps.graph.local.LocalRecordReader;
import com.aliyun.odps.graph.local.LocalRecordWriter;
import com.aliyun.odps.graph.local.LocalVertexMutations;
import com.aliyun.odps.graph.local.RuntimeContext;
import com.aliyun.odps.graph.local.SQLRecord;
import com.aliyun.odps.graph.local.TaskContextImpl;
import com.aliyun.odps.graph.local.master.Master;
import com.aliyun.odps.graph.local.message.MsgManager;
import com.aliyun.odps.graph.local.utils.LocalGraphRunUtils;
import com.aliyun.odps.graph.utils.VerifyUtils;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.io.WritableUtils;
import com.aliyun.odps.utils.ReflectionUtils;

@SuppressWarnings("rawtypes")
public class Worker<VERTEX_ID extends WritableComparable<?>, VERTEX_VALUE extends Writable, EDGE_VALUE extends Writable, MESSAGE extends Writable, VALUE extends Writable> {

  private static Log LOG = LogFactory.getLog(Worker.class);

  private List<Aggregator> mAggregators;
  private List<Writable> mAggregatorValues;

  private Master master;
  private Counters mCounters;
  private RuntimeContext mCtx;
  private InputSplit mInput;
  private JobConf mJob;
  private List<Writable> mLastAggregatorValues;
  private Map<Vertex, Iterable<Writable>>
      mLastStepMessage =
      new HashMap<Vertex, Iterable<Writable>>();
  private Map<String, TableInfo> mOutputs;
  private Map<VERTEX_ID, LocalVertexMutations> mVertexMutations;
  private GraphTaskAttemptID mTaskAttemptID;
  private TaskContextImpl mTaskContext;
  private WorkerComputer mWorkerComputer;

  private int mWorkerID;
  private int mWorkerNum;
  private Map<String, LocalRecordWriter> mWriters;

  private Map<VERTEX_ID, Vertex> vertices = new HashMap<VERTEX_ID, Vertex>();

  private Writable mWorkerValue;

  private MsgManager mMsgManager;

  private Combiner mCombiner;

  @SuppressWarnings("unchecked")
  public Worker(JobConf job, RuntimeContext ctx, Master m,
                GraphTaskAttemptID taskAttemptID, int workerID, int workerNum,
                InputSplit input, Map<String, TableInfo> outputs)
      throws InstantiationException, IllegalAccessException, IOException,
             ClassNotFoundException {

    mJob = job;
    mCtx = ctx;
    master = m;
    mTaskAttemptID = taskAttemptID;
    mWorkerID = workerID;
    mWorkerNum = workerNum;

    mInput = input;
    mOutputs = outputs;
    mCounters = new Counters();

    mAggregators = LocalGraphRunUtils.getAggregator(mJob);
    mMsgManager = new MsgManager();

    mTaskContext = new TaskContextImpl(mCtx, mJob, this, mWorkerID, mWorkerNum,
                                       mOutputs, mCounters);
    mVertexMutations = new HashMap<VERTEX_ID, LocalVertexMutations>();

  }

  public boolean allVertexVoltHalt() {
    boolean ret = true;
    for (Vertex v : vertices.values()) {
      ret = ret && v.isHalted();
    }
    return ret && (!mMsgManager.hasNextStepMessages());
  }

  private void initCombiner() {
    Class<? extends Combiner> combinerCls = mJob.getCombinerClass();
    if (combinerCls != null) {
      try {
        mCombiner = combinerCls.newInstance();
        mCombiner.configure(mJob);
      } catch (Exception e) {
        throw new RuntimeException(
            "exception occored when Instantiate combiner ", e);
      }
    } else {
      mCombiner = null;
    }

  }

  @SuppressWarnings("unchecked")
  private void initWorkerComputer() throws IOException {
    try {
      Class<? extends WorkerComputer> workerComputerClass = mJob
          .getWorkerComputerClass();

      mWorkerComputer = workerComputerClass.newInstance();

      List<Class<?>> workerValueClass = ReflectionUtils
          .<WorkerComputer>getTypeArguments(WorkerComputer.class,
                                            workerComputerClass);

      if (workerValueClass.size() > 1) {
        throw new IOException("more than ONE workerValue Type Declared");
      } else if (workerValueClass.size() == 0) {
        mWorkerValue = NullWritable.get();
      } else if (workerValueClass.get(0) == null) {
        mWorkerValue = NullWritable.get();
      } else {
        mWorkerValue = (Writable) ReflectionUtils.newInstance(workerValueClass.get(0), mJob);
      }
      mWorkerComputer.setup(mTaskContext, mWorkerValue);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void initAggregatorValues() throws IOException {
    mLastAggregatorValues = new ArrayList<Writable>();
    for (Aggregator agg : mAggregators) {
      mLastAggregatorValues.add(agg.createStartupValue(mTaskContext));
    }
  }

  @SuppressWarnings("unchecked")
  public void loadGraph() throws IOException {

    Class<? extends GraphLoader> loaderClass = mJob.getGraphLoaderClass();

    GraphLoader graphLoader = ReflectionUtils.newInstance(loaderClass, mJob);

    graphLoader.setup(mTaskContext.getConfiguration(),
                      mTaskContext.getWorkerId(), mInput.getTable(), mTaskContext);

    BaseRecordReader<LongWritable, WritableRecord> reader;

    if (mInput != null && mInput.getTable() != null) {
      String fullName = mInput.getTable().getProjectName() + "."
                        + mInput.getTable().getTableName();
      //File inputDir = mCtx.getInputDir(fullName, mInput.getPartSpec());
      File tableInfo = mInput.getFile().getParentFile();
      while (!tableInfo.getName().equals(mInput.getTable().getTableName())) {
        tableInfo = tableInfo.getParentFile();
      }
      reader = new LocalRecordReader(mInput.getFile().getParentFile(), tableInfo,
                                     mCounters.findCounter(COUNTER.TASK_INPUT_RECORD),
                                     mCounters.findCounter(COUNTER.TASK_INPUT_BYTE));
    } else {
      reader = new EmptyRecordReader();
    }

    mWriters = new HashMap<String, LocalRecordWriter>();
    for (String label : mOutputs.keySet()) {
      Counter outputRecordCounter = mCounters
          .findCounter(COUNTER.TASK_OUTPUT_RECORD);
      Counter outputByteCounter = mCounters
          .findCounter(COUNTER.TASK_OUTPUT_BYTE);
      LocalRecordWriter writer = new LocalRecordWriter(new File(mCtx.getOutputDir(label),
                                                                this.mTaskAttemptID.toString()),
                                                       outputRecordCounter,
                                                       outputByteCounter);
      mWriters.put(label, writer);
    }

    while (reader.nextKeyValue()) {
      LongWritable recordNum = new LongWritable();
      recordNum.set((reader.getCurrentKey().get()));
      graphLoader.load(recordNum, ((SQLRecord) reader.getCurrentValue()).clone(),
                       mTaskContext);
    }

    reader.close();

    mTaskContext.setOutputWriters(mWriters);
  }

  @SuppressWarnings("unchecked")
  public void init() throws IOException {
    initCombiner();
    initWorkerComputer();
    initAggregatorValues();

    for (Vertex v : vertices.values()) {
      v.setup(mTaskContext);
    }

  }

  public void processNextStep() throws IOException {
    mAggregatorValues = new ArrayList<Writable>();
    for (int i = 0; i < mAggregators.size(); ++i) {
      Writable initAggregatorValue = mAggregators.get(i)
          .createInitialValue(mTaskContext);
      if (initAggregatorValue == null) {
        throw new RuntimeException("ODPS-0730001: " + mAggregators.get(i).getClass().getName()
                                   + " createInitialValue return null");
      }
      mAggregatorValues.add(initAggregatorValue);
    }
    mMsgManager.nextSuperStep(mCtx);
  }

  @SuppressWarnings("unchecked")
  public void Compute() throws IOException {

    prepareMsg();
    for (Vertex v : vertices.values()) {
      Iterable<Writable> msg = mLastStepMessage.get(v);
      if (v.isHalted() && msg.iterator().hasNext()) {
        v.wakeUp();
      }

      if (!v.isHalted()) {
        v.compute(mTaskContext, msg);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void processMutation(VERTEX_ID id, LocalVertexMutations mutations,
                              VertexResolver vertexResolver) throws IOException {
    Vertex v = vertices.get(id);
    boolean hasMessage = mMsgManager.hasMessageForVertex(mCtx,
                                                         master.getSuperStep(), id);
    if (vertexResolver == null) {
      throw new IOException(
          "ODPS-0730001: encounter mutations in compute but not set the mutation resolver.");
    }
    Vertex new_v = vertexResolver.resolve(id, v, mutations, hasMessage);
    if (new_v == null) {
      vertices.remove(id);
    } else {
      VerifyUtils.verifyVertex(new_v);
      vertices.put(id, new_v);
    }
  }

  @SuppressWarnings("unchecked")
  public void processWorkerMutations(VertexResolver vertexResolver)
      throws IOException {
    HashSet<VERTEX_ID> mutationIDs = new HashSet<VERTEX_ID>();
    mutationIDs.addAll(mVertexMutations.keySet());

    for (WritableComparable<?> id : mMsgManager.getVertexIDList()) {
      if (vertices.get(id) == null) {
        mutationIDs.add((VERTEX_ID) id);
      }
    }

    for (VERTEX_ID id : mutationIDs) {
      processMutation(id, mVertexMutations.get(id), vertexResolver);
    }
    mVertexMutations = new HashMap<VERTEX_ID, LocalVertexMutations>();
  }

  public List<Writable> getAggregatorValues() {
    return mAggregatorValues;
  }

  public Counters getCounters() {
    return mCounters;
  }

  public long getEgeNumber() {
    long egeNumber = 0;
    for (Vertex v : vertices.values()) {
      egeNumber += v.getNumEdges();
    }
    return egeNumber;
  }

  public List<Writable> getLastAggregatedValue() {
    return mLastAggregatorValues;
  }

  public Master getMaster() {
    return master;
  }

  public WorkerContext getTaskContext() {
    return mTaskContext;
  }

  public long getVertexNumber() {
    return vertices.size();
  }

  public LocalVertexMutations getVertexMutations(VERTEX_ID id) {
    LocalVertexMutations ret = mVertexMutations.get(id);
    if (ret == null) {
      ret = new LocalVertexMutations();
      mVertexMutations.put(id, ret);
    }
    return ret;
  }

  public List<Writable> partialAggregate() {
    return mAggregatorValues;
  }

  public void pushMsg(RuntimeContext context, long superStep,
                      WritableComparable<?> vertexId, Writable msg) {
    mMsgManager.pushMsg(context, superStep, vertexId, msg);
  }

  public void close() throws IOException {
    for (LocalRecordWriter writer : mWriters.values()) {
      writer.close();
    }
    mWriters = null;

    FileUtils.writeStringToFile(
        new File(mCtx.getCounterDir(), String.valueOf(this.mTaskAttemptID
                                                          .getTaskId())), mCounters.toString());
    LOG.debug(mCounters);
  }

  @SuppressWarnings("unchecked")
  public void cleanup() throws IOException {
    for (Vertex v : vertices.values()) {
      v.cleanup(mTaskContext);
    }
    mWorkerComputer.cleanup(mTaskContext);
  }

  @SuppressWarnings("unchecked")
  private Iterable<Writable> combineMsg(WritableComparable id,
                                        Iterable<Writable> msgs) throws IOException {
    if (mCombiner != null) {
      Writable combineMsg = null;
      for (Writable msg : msgs) {
        if (combineMsg == null) {
          combineMsg = msg;
        } else {
          mCombiner.combine(id, combineMsg, msg);
        }
      }
      ArrayList<Writable> combinedMsgs = new ArrayList<Writable>();
      if (combineMsg != null) {
        combinedMsgs.add(combineMsg);
      }
      return combinedMsgs;
    } else {
      return msgs;
    }
  }

  private void prepareMsg() throws IOException {
    long superStep = master.getSuperStep();
    LOG.debug("worker super step " + superStep + ", vertices count "
              + vertices.size());
    mLastStepMessage.clear();
    for (Vertex v : vertices.values()) {
      Iterable<Writable> msgs = mMsgManager
          .popMsges(mCtx, superStep, v.getId());
      if (mCombiner != null) {
        msgs = combineMsg(v.getId(), msgs);
      }
      mLastStepMessage.put(v, msgs);
    }

  }

  public void setLastAggregatedValue(List<Writable> lastAggrValues) {
    mLastAggregatorValues = new ArrayList<Writable>(lastAggrValues.size());
    for (int i = 0; i < lastAggrValues.size(); ++i) {
      Writable value = null;
      if (lastAggrValues.get(i) != null) {
        value = WritableUtils.clone(lastAggrValues.get(i), mJob);
      }
      mLastAggregatorValues.add(value);
    }
  }

  public void setTotalNumVerticesAndEdges(int totalVertices, int totalEdge) {
    mTaskContext.setTotalNumVertices(totalVertices);
    mTaskContext.setTotalNumEdges(totalEdge);
  }

  public Writable getWorkerValue() {
    return mWorkerValue;
  }

}
