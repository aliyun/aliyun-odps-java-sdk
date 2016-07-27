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

package com.aliyun.odps.graph.local;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.Aggregator;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.GRAPH_CONF;
import com.aliyun.odps.graph.JobConf;
import com.aliyun.odps.graph.JobConf.JobState;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.common.COMMON_GRAPH_CONF;
import com.aliyun.odps.graph.local.utils.LocalGraphRunUtils;
import com.aliyun.odps.graph.local.worker.Worker;
import com.aliyun.odps.graph.utils.VerifyUtils;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.local.common.utils.SchemaUtils;

@SuppressWarnings("rawtypes")
public class TaskContextImpl<VERTEX_ID extends WritableComparable<?>, VERTEX_VALUE extends Writable, EDGE_VALUE extends Writable, MESSAGE extends Writable>
    extends ComputeContext<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> {

  private static Log LOG = LogFactory.getLog(TaskContextImpl.class);
  private List<Aggregator> mAggregators;

  private Counters mCounters;
  private RuntimeContext mCtx;
  private JobConf mJob;
  private Map<String, WritableRecord> mOutputRecords;
  private Map<String, TableInfo> mOutputs;

  private int mTaskNum;
  long mTotalNumEdges;
  long mTotalNumVertices;
  private Worker mWorker;
  private int mWorkerID;

  private Map<String, LocalRecordWriter> mWriters;

  private int maxUserDefinedCountersNum = 64;
  private Map<String, Counter> userCounters =
      new HashMap<String, Counter>();

  public TaskContextImpl(RuntimeContext ctx, JobConf conf, Worker worker,
                         int workerID, int taskNum, Map<String, TableInfo> outputs,
                         Counters counters) {
    mWorker = worker;
    mCtx = ctx;
    mJob = conf;
    mWorkerID = workerID;
    mTaskNum = taskNum;
    mOutputs = outputs;
    mCounters = counters;

    mAggregators = LocalGraphRunUtils.getAggregator(mJob);

    mOutputRecords = new HashMap<String, WritableRecord>();

    maxUserDefinedCountersNum =
        mJob.getInt(COMMON_GRAPH_CONF.JOB_MAX_USER_DEFINED_COUNTERS_NUM, 64);
  }

  @SuppressWarnings("unchecked")
  public LocalVertexMutations getRealVertexMutations(VERTEX_ID id) {
    if (mJob.getRuntimePartitioning()) {
      return mWorker.getMaster().getVertexMutations(id);
    } else {
      return mWorker.getVertexMutations(id);
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void addVertexRequest(
      Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> vertex)
      throws IOException {
    VerifyUtils.verifyVertex(vertex);
    LocalVertexMutations vertexMutations = getRealVertexMutations(vertex
                                                                      .getId());
    vertexMutations.addVertex(vertex);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void addEdgeRequest(VERTEX_ID sourceVertexId,
                             Edge<VERTEX_ID, EDGE_VALUE> edge) throws IOException {
    VerifyUtils.verifyVertexId(sourceVertexId);
    VerifyUtils.verifyVertexEdge(edge);
    LocalVertexMutations vertexMutations = getRealVertexMutations(sourceVertexId);
    vertexMutations.addEdge(edge);

  }

  @SuppressWarnings("unchecked")
  @Override
  public void removeEdgeRequest(VERTEX_ID sourceVertexId,
                                VERTEX_ID targetVertexId) throws IOException {
    VerifyUtils.verifyVertexId(sourceVertexId);
    VerifyUtils.verifyVertexId(targetVertexId);
    LocalVertexMutations vertexMutations = getRealVertexMutations(sourceVertexId);
    vertexMutations.removeEdge(targetVertexId);
  }

  @Override
  public void removeVertexRequest(VERTEX_ID vertexId) throws IOException {
    VerifyUtils.verifyVertexId(vertexId);
    LocalVertexMutations vertexMutations = getRealVertexMutations(vertexId);
    vertexMutations.removeVertex();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void sendMessage(VERTEX_ID destVertexID, MESSAGE msg)
      throws IOException {
    if (!mJob.getRuntimePartitioning()) {
      throw new RuntimeException(
          "ODPS-0730001: vertex partitioning disabled, cannot send message");
    }
    if (msg == null) {
      throw new IllegalArgumentException(
          "ODPS-0730001: sendMessage: Cannot send null message to "
          + destVertexID);
    }
    mWorker.getMaster().pushMsg(mCtx, getSuperstep() + 1, destVertexID, msg);
  }

  @Override
  public void sendMessage(Iterable<VERTEX_ID> destVertexIDs, MESSAGE msg)
      throws IOException {
    for (VERTEX_ID vertexId : destVertexIDs) {
      sendMessage(vertexId, msg);
    }
  }

  /**
   * 发送消息到给定点的所有邻接点，在下一个超步中会传给这些邻接点的
   * {@link Vertex#compute(ComputeContext, Iterable)} 方法进行处理.
   *
   * @param msg
   *     待发送的消息
   * @throws IOException
   */
  @Override
  public void sendMessageToNeighbors(
      Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE, MESSAGE> vertex, MESSAGE msg)
      throws IOException {
    if (vertex.hasEdges()) {
      for (Edge<VERTEX_ID, EDGE_VALUE> edge : vertex.getEdges()) {
        sendMessage(edge.getDestVertexId(), msg);
      }
    }
  }

  @Override
  public void aggregate(Object item) throws IOException {
    for (int i = 0; i < mAggregators.size(); ++i) {
      aggregate(i, item);
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void aggregate(int aggregatorIndex, Object value) throws IOException {
    List<Writable> aggregatorValues = mWorker.getAggregatorValues();
    Writable aggValue = aggregatorValues.get(aggregatorIndex);
    mAggregators.get(aggregatorIndex).aggregate(aggValue, value);
  }

  private WritableRecord createOutputRecord(String label) throws IOException {
    Column[] cols = SchemaUtils.readSchema(mCtx.getOutputDir(label)).getCols();
    return new SQLRecord(cols);
  }


  @Override
  public Configuration getConfiguration() {
    return new JobConf(mJob, JobState.RUNNING);
  }

  @Override
  public Counter getCounter(Enum<?> name) {
    if (name == null) {
      throw new RuntimeException("ODPS-0730001: Counter name must be not null.");
    }
    return getCounter(name.getDeclaringClass().getName(), name.toString());
  }

  @Override
  public Counter getCounter(String group, String name) {
    String key = group + "#" + name;
    if (userCounters.containsKey(key)) {
      return userCounters.get(key);
    }
    checkUserDefinedCounters(group, name);
    Counter counter = mCounters.findCounter(group, name);
    userCounters.put(key, counter);
    return counter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <VALUE extends Writable> VALUE getLastAggregatedValue(
      int aggregatorIndex) {
    return ((VALUE) mWorker.getLastAggregatedValue().get(aggregatorIndex));
  }

  @Override
  public long getMaxIteration() {
    return getConfiguration().getLong(GRAPH_CONF.MAX_ITERATION, -1);
  }

  @Override
  public int getNumWorkers() {
    return mTaskNum;
  }

  @Override
  public TableInfo getOutputTable() throws IOException {
    return getOutputTable("");
  }

  @Override
  public TableInfo getOutputTable(String label) throws IOException {
    return mOutputs.get(label);
  }

  @Override
  public long getSuperstep() {
    return mWorker.getMaster().getSuperStep();
  }

  @Override
  public long getTotalNumEdges() {
    return mTotalNumEdges;
  }

  @Override
  public long getTotalNumVertices() {
    return mTotalNumVertices;
  }

  @Override
  public Writable getWorkerValue() {
    return mWorker.getWorkerValue();
  }

  @Override
  public Writable getComputeValue() {
    return null;
  }

  @Override
  public int getWorkerId() {
    return mWorkerID;
  }

  @Override
  public long getWorkerNumEdges() {
    return mWorker.getEgeNumber();
  }

  @Override
  public long getWorkerNumVertices() {
    return mWorker.getVertexNumber();
  }

  @Override
  public void progress() {
    LOG.debug("Graph Local Mode Just Mock progress method. Not Calculate Time");
  }

  public void closeWriters() throws IOException {
    for (LocalRecordWriter writer : mWriters.values()) {
      writer.close();
    }
  }


  public void setOutputWriters(Map<String, LocalRecordWriter> writers) {
    this.mWriters = writers;
  }

  public void setTotalNumEdges(long totalEdges) {
    mTotalNumEdges = totalEdges;
  }

  public void setTotalNumVertices(long totalVertices) {
    mTotalNumVertices = totalVertices;
  }

  @Override
  public void write(String label, Writable... fieldVals) throws IOException {
    LocalRecordWriter writer = mWriters.get(label);

    if (writer == null) {
      throw new IOException("The label " + label + " is not found in output");
    }

    if (mOutputRecords.get(label) == null) {
      mOutputRecords.put(label, createOutputRecord(label));
    }

    WritableRecord record = mOutputRecords.get(label);
    record.set(fieldVals);
    writer.write(record);

  }

  @Override
  public void write(Writable... fieldVals) throws IOException {
    write("", fieldVals);
  }

  private void checkUserDefinedCounters(String groupName,
                                        String counterName) {
    if (counterName == null || counterName.isEmpty()) {
      throw new RuntimeException(
          "ODPS-0730001: CounterName must be not null or empty.");
    }
    if (groupName == null || groupName.isEmpty()) {
      throw new RuntimeException(
          "ODPS-0730001: groupName must be not null or empty.");
    }

    if (groupName.contains("#")) {
      throw new RuntimeException("ODPS-0730001: Group name: " + groupName
                                 + " is invalid, It should not contain '#'");
    }
    if (counterName.contains("#")) {
      throw new RuntimeException("ODPS-0730001: Counter name: " + counterName
                                 + " is invalid, It should not contain '#'");
    }

    int maxLength = 100;

    if (groupName.length() + counterName.length() > maxLength) {
      throw new RuntimeException("ODPS-0730001: Group name '" + groupName
                                 + "' and Counter name '" + counterName
                                 + "' is too long, sum of their length must <= " + maxLength);
    }
    if (userCounters.size() >= maxUserDefinedCountersNum) {
      throw new RuntimeException(
          "ODPS-0730001: Total num of user defined counters is too more, must be <= "
          + maxUserDefinedCountersNum);
    }
  }

  @Override
  public byte[] readCacheFile(String resourceName) throws IOException {
    return IOUtils.toByteArray(readCacheFileAsStream(resourceName));
  }

  @Override
  public BufferedInputStream readCacheFileAsStream(String resourceName)
      throws IOException {
    File file = new File(mCtx.getResourceDir(), resourceName);
    return new BufferedInputStream(new FileInputStream(file));
  }

  @Override
  public Iterable<byte[]> readCacheArchive(String resourceName)
      throws IOException {
    return readCacheArchive(resourceName, "");
  }

  @Override
  public Iterable<byte[]> readCacheArchive(String resourceName,
                                           String relativePath) throws IOException {
    File baseDir = new File(mCtx.getResourceDir(), resourceName);
    File dir = new File(baseDir, relativePath);
    File[] files = dir.listFiles();
    final List<byte[]> list = new ArrayList<byte[]>();
    for (File file : files) {
      list.add(IOUtils.toByteArray(
          new BufferedInputStream(new FileInputStream(file))));
    }
    return new Iterable<byte[]>() {
      @Override
      public Iterator<byte[]> iterator() {
        return list.iterator();
      }
    };
  }

  @Override
  public Iterable<BufferedInputStream> readCacheArchiveAsStream(
      String resourceName) throws IOException {
    return readCacheArchiveAsStream(resourceName, "");
  }

  @Override
  public Iterable<BufferedInputStream> readCacheArchiveAsStream(
      String resourceName, String relativePath) throws IOException {
    File baseDir = new File(mCtx.getResourceDir(), resourceName);
    File dir = new File(baseDir, relativePath);
    File[] files = dir.listFiles();
    final List<BufferedInputStream> list = new ArrayList<BufferedInputStream>();
    for (File file : files) {
      list.add(new BufferedInputStream(new FileInputStream(file)));
    }
    return new Iterable<BufferedInputStream>() {
      @Override
      public Iterator<BufferedInputStream> iterator() {
        return list.iterator();
      }
    };
  }

  @Override
  public Iterable<WritableRecord> readResourceTable(String resourceName)
      throws IOException {
    final File tableDir = new File(mCtx.getResourceDir(), resourceName);
    if (!tableDir.exists()) {
      throw new RuntimeException("resource " + resourceName + " not found!");
    }
    if (tableDir.isFile()) {
      throw new RuntimeException("resource " + resourceName + " is not a table resource!");
    }
    final List<File> dataFiles = new ArrayList<File>();
    LocalRunUtils.listAllDataFiles(tableDir, dataFiles);

    return new Iterable<WritableRecord>() {
      @Override
      public Iterator<WritableRecord> iterator() {
        return new WrappedRecordIterator(tableDir, dataFiles);
      }

    };
  }

  private class WrappedRecordIterator implements Iterator<WritableRecord> {

    LocalRecordReader reader;
    WritableRecord current;
    boolean fetched;
    Iterator<File> fileIter;
    File tableDir;

    WrappedRecordIterator(File tableDir, List<File> dataFiles) {
      this.tableDir = tableDir;
      this.fileIter = dataFiles.iterator();
    }

    @Override
    public boolean hasNext() {
      if (fetched) {
        return current != null;
      }
      // Fetch new one
      try {
        fetch();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return current != null;
    }

    private void fetch() throws IOException {
      fetched = true;
      while (true) {
        // first time or next reader
        if (reader == null) {
          if (!fileIter.hasNext()) {
            current = null;
            return;
          }
          File dataFile = fileIter.next();
          reader = new LocalRecordReader(dataFile.getParentFile(),
                                         tableDir, null, null);
        }
        // read next
        if (reader.nextKeyValue()) {
          current = reader.getCurrentValue();
          return;
        } else {
          reader = null;
          continue;
        }
      }
    }

    @Override
    public WritableRecord next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      fetched = false;
      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  @Override
  public TableInfo getResourceTable(String resourceName) throws IOException {
    File dir = new File(mCtx.getResourceDir(), resourceName);
    if (!dir.exists()) {
      throw new RuntimeException("resource " + resourceName + " not found!");
    }
    if (dir.isFile()) {
      throw new RuntimeException("resource " + resourceName + " is not a table resource!");
    }
    TableMeta meta = SchemaUtils.readSchema(dir);
    return WareHouse.getInstance().getReferencedTable(meta.getProjName(), resourceName);
  }
}
