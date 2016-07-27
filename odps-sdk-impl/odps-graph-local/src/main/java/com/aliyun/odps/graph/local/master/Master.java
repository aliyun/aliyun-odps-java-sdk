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

package com.aliyun.odps.graph.local.master;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.CounterGroup;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.Aggregator;
import com.aliyun.odps.graph.JobConf;
import com.aliyun.odps.graph.Partitioner;
import com.aliyun.odps.graph.VertexResolver;
import com.aliyun.odps.graph.local.COUNTER;
import com.aliyun.odps.graph.local.GraphTaskAttemptID;
import com.aliyun.odps.graph.local.InputSplit;
import com.aliyun.odps.graph.local.LocalVertexMutations;
import com.aliyun.odps.graph.local.RuntimeContext;
import com.aliyun.odps.graph.local.utils.LocalGraphRunUtils;
import com.aliyun.odps.graph.local.worker.Worker;
import com.aliyun.odps.graph.utils.VerifyUtils;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.local.common.FileSplit;
import com.aliyun.odps.utils.ReflectionUtils;

@SuppressWarnings("rawtypes")
public class Master<VERTEX_ID extends WritableComparable<?>, VERTEX_VALUE extends Writable, EDGE_VALUE extends Writable, MESSAGE extends Writable, AGGR_INFO extends Writable, VALUE extends Writable> {

  private static Log LOG = LogFactory.getLog(Master.class);

  JobConf mJob;
  RuntimeContext mCtx;
  List<FileSplit> mInputs;
  Map<String, TableInfo> mOutputs;

  private Partitioner<VERTEX_ID> p;
  private List<AGGR_INFO> mLastAggregatorValues;
  int superStep = -1;
  int maxIteration;

  List<Worker> mWorkers = new ArrayList<Worker>();
  private Counters mCounters;
  int totalVertex = 0;
  int totalEdge = 0;

  public Master(JobConf job, RuntimeContext ctx, List<FileSplit> inputs,
                Map<String, TableInfo> outputs) throws Exception {

    mJob = job;
    mCtx = ctx;
    mInputs = inputs;
    mOutputs = outputs;
    mLastAggregatorValues = new ArrayList<AGGR_INFO>();

    mCounters = ctx.getCounters();

    init();

  }

  @SuppressWarnings("unchecked")
  private void init() throws InstantiationException, IllegalAccessException,
                             IOException, ClassNotFoundException, NoSuchFieldException {
    VerifyUtils.verifyGraphConf(mJob);

    ReflectionUtils.setField(mJob, "state", JobConf.JobState.RUNNING);

    maxIteration = mJob.getMaxIteration();
    int usrWorkerNum = mJob.getNumWorkers();
    int workerNum = mInputs.size() < usrWorkerNum ? usrWorkerNum : mInputs.size();
    LOG.info("worker num :" + workerNum);

    p = LocalGraphRunUtils.createPartitioner(mJob);

    for (int workerCount = 0; workerCount < workerNum; ++workerCount) {

      FileSplit input = null;
      if (workerCount < mInputs.size()) {
        input = mInputs.get(workerCount);
      }

      InputSplit inputSplit = null;
      if (input != null && input != FileSplit.NullSplit) {
        inputSplit = (InputSplit) input;
      } else {
        inputSplit = InputSplit.NullSplit;
      }
      GraphTaskAttemptID taskAttemptID = new GraphTaskAttemptID(
          mCtx.getJobId(), workerCount, 0);

      Worker w = new Worker(mJob, mCtx, this, taskAttemptID, workerCount,
                            workerNum, inputSplit, mOutputs);

      mWorkers.add(w);
    }
    initCounters();
  }

  private void initCounters() {
    mCounters.findCounter(COUNTER.TASK_INPUT_BYTE);
    mCounters.findCounter(COUNTER.TASK_INPUT_RECORD);
    mCounters.findCounter(COUNTER.TASK_OUTPUT_BYTE);
    mCounters.findCounter(COUNTER.TASK_OUTPUT_RECORD);
  }

  private void initGraph() throws IOException {
    for (Worker w : mWorkers) {
      w.loadGraph();
    }

    VertexResolver vertexResolver = LocalGraphRunUtils
        .createLoadingVertexResolver(mJob);
    processMutations(vertexResolver);

    if (totalVertex == 0) {
      throw new IOException("ODPS-0730001: No vertices in the graph, exiting.");
    }

  }

  /*
   * if RuntimePartion is True add mutation to master and master use
   * worker.processmutations
   * 
   * if is False add mutation to worker and worker use
   * worker.processWorkermutations -> worker.processmutations no need send the
   * mutations to worker
   * 
   * message avaliable only the runtimePartition is True
   */

  private void processMutations(VertexResolver vertexResolver)
      throws IOException {
    for (Worker worker : mWorkers) {
      worker.processWorkerMutations(vertexResolver);
    }

    totalVertex = 0;
    totalEdge = 0;
    for (Worker w : mWorkers) {
      totalVertex += w.getVertexNumber();
      totalEdge += w.getEgeNumber();
    }

    for (Worker w : mWorkers) {
      w.setTotalNumVerticesAndEdges(totalVertex, totalEdge);
    }
  }

  private void WorkerSetup() throws IOException {
    initGraph();
    for (Worker w : mWorkers) {
      w.init();
    }
  }

  @SuppressWarnings("unchecked")
  private boolean Aggregate() throws IOException {
    List<Aggregator> aggregators = LocalGraphRunUtils.getAggregator(mJob);

    if (aggregators.size() > 0) {
      boolean terminate = false;
      List<AGGR_INFO> totalAggregatorValue = null;

      for (int i = 0; i < mWorkers.size(); ++i) {

        List<AGGR_INFO> partialValue = mWorkers.get(i).partialAggregate();

        if (i == 0) {
          totalAggregatorValue = partialValue;
        } else {
          for (int j = 0; j < partialValue.size(); ++j) {
            aggregators.get(j).merge(totalAggregatorValue.get(j),
                                     partialValue.get(j));
          }
        }
      }

      for (int i = 0; i < totalAggregatorValue.size(); ++i) {
        terminate |= aggregators.get(i).terminate(
            mWorkers.get(0).getTaskContext(), totalAggregatorValue.get(i));
      }
      mLastAggregatorValues = totalAggregatorValue;
      for (int i = 0; i < mWorkers.size(); ++i) {
        mWorkers.get(i).setLastAggregatedValue(mLastAggregatorValues);
      }
      return terminate;
    }
    return false;
  }

  private void close() throws IOException {
    for (Worker w : mWorkers) {
      for (CounterGroup cg : w.getCounters()) {
        for (Counter c : cg) {
          mCounters.findCounter(cg.getName(), c.getDisplayName()).increment(
              c.getValue());
        }
      }
    }
    FileUtils.writeStringToFile(
        new File(mCtx.getCounterDir(), mCtx.getJobId()), mCounters.toString());
    LOG.debug(mCounters.toString());
    System.out.println(mCounters.toString());
  }

  public void run() throws IOException {

    WorkerSetup();

    while ((maxIteration < 0) || (superStep + 1 < maxIteration)) {

      // check volt halt
      boolean allVoltHalt = true;
      for (Worker w : mWorkers) {
        allVoltHalt = allVoltHalt && w.allVertexVoltHalt();
      }
      if (allVoltHalt) {
        break;
      }

      LOG.debug("master super step " + (superStep + 1));

      processMutations(LocalGraphRunUtils.createSuperstepVertexResolver(mJob));

      ++superStep;

      for (Worker w : mWorkers) {
        w.processNextStep();
      }

      for (Worker w : mWorkers) {
        w.Compute();
      }

      if (Aggregate()) {
        break;
      }
    }

    for (Worker w : mWorkers) {
      w.cleanup();
    }

    for (Worker w : mWorkers) {
      w.close();
    }
    close();
  }

  private Worker getWorkerByVertexID(VERTEX_ID vertex_id) {
    int worker_id = p.getPartition(vertex_id, mWorkers.size());
    return mWorkers.get(worker_id);
  }

  public long getSuperStep() {
    return superStep;
  }

  @SuppressWarnings("unchecked")
  public LocalVertexMutations getVertexMutations(VERTEX_ID id) {
    return getWorkerByVertexID(id).getVertexMutations(id);
  }

  @SuppressWarnings("unchecked")
  public void pushMsg(RuntimeContext context, long superStep,
                      VERTEX_ID vertexId, Writable msg) {
    getWorkerByVertexID(vertexId).pushMsg(context, superStep, vertexId, msg);

  }

}
