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

package com.aliyun.odps.examples.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.Aggregator;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.GraphJob;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Tuple;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.local.common.WareHouse;

/**
 * Definition from Wikipedia:
 * In the mathematical theory of directed graphs, a graph is said
 * to be strongly connected if every vertex is reachable from every
 * other vertex. The strongly connected components of an arbitrary
 * directed graph form a partition into subgraphs that are themselves
 * strongly connected.
 *
 * Algorithms with four phases as follows.
 * 1. Transpose Graph Formation: Requires two supersteps. In the first
 * superstep, each vertex sends a message with its ID to all its outgoing
 * neighbors, which in the second superstep are stored in transposeNeighbors.
 *
 * 2. Trimming: Takes one superstep. Every vertex with only in-coming or
 * only outgoing edges (or neither) sets its colorID to its own ID and
 * becomes inactive. Messages subsequently sent to the vertex are ignored.
 *
 * 3. Forward-Traversal: There are two sub phases: Start and Rest. In the
 * Start phase, each vertex sets its colorID to its own ID and propagates
 * its ID to its outgoing neighbors. In the Rest phase, vertices update
 * their own colorIDs with the minimum colorID they have seen, and propagate
 * their colorIDs, if updated, until the colorIDs converge.
 * Set the phase to Backward-Traversal when the colorIDs converge.
 *
 * 4. Backward-Traversal: We again break the phase into Start and Rest.
 * In Start, every vertex whose ID equals its colorID propagates its ID to
 * the vertices in transposeNeighbors and sets itself inactive. Messages
 * subsequently sent to the vertex are ignored. In each of the Rest phase supersteps,
 * each vertex receiving a message that matches its colorID: (1) propagates
 * its colorID in the transpose graph; (2) sets itself inactive. Messages
 * subsequently sent to the vertex are ignored. Set the phase back to Trimming
 * if not all vertex are inactive.
 *
 * http://ilpubs.stanford.edu:8090/1077/3/p535-salihoglu.pdf
 */
public class StronglyConnectedComponents {

  public final static int STAGE_TRANSPOSE_1 = 0;
  public final static int STAGE_TRANSPOSE_2 = 1;
  public final static int STAGE_TRIMMING = 2;
  public final static int STAGE_FW_START = 3;
  public final static int STAGE_FW_REST = 4;
  public final static int STAGE_BW_START = 5;
  public final static int STAGE_BW_REST = 6;

  /**
   * The value is composed of component id, incoming neighbors,
   * active status and updated status.
   */
  public static class MyValue implements Writable {

    LongWritable sccID;// strongly connected component id
    Tuple inNeighbors; // transpose neighbors

    BooleanWritable active; // vertex is active or not
    BooleanWritable updated; // sccID is updated or not

    public MyValue() {
      this.sccID = new LongWritable(Long.MAX_VALUE);
      this.inNeighbors = new Tuple();
      this.active = new BooleanWritable(true);
      this.updated = new BooleanWritable(false);
    }

    public void setSccID(LongWritable sccID) {
      this.sccID = sccID;
    }

    public LongWritable getSccID() {
      return this.sccID;
    }

    public void setInNeighbors(Tuple inNeighbors) {
      this.inNeighbors = inNeighbors;
    }

    public Tuple getInNeighbors() {
      return this.inNeighbors;
    }

    public void addInNeighbor(LongWritable neighbor) {
      this.inNeighbors.append(new LongWritable(neighbor.get()));
    }

    public boolean isActive() {
      return this.active.get();
    }

    public void setActive(boolean status) {
      this.active.set(status);
    }

    public boolean isUpdated() {
      return this.updated.get();
    }

    public void setUpdated(boolean update) {
      this.updated.set(update);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.sccID.write(out);
      this.inNeighbors.write(out);
      this.active.write(out);
      this.updated.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.sccID.readFields(in);
      this.inNeighbors.readFields(in);
      this.active.readFields(in);
      this.updated.readFields(in);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("sccID: " + sccID.get());
      sb.append(" inNeighbores: " + inNeighbors.toDelimitedString(','));
      sb.append(" active: " + active.get());
      sb.append(" updated: " + updated.get());
      return sb.toString();
    }

  }

  public static class SCCVertex extends
                                Vertex<LongWritable, MyValue, NullWritable, LongWritable> {

    public SCCVertex() {
      this.setValue(new MyValue());
    }

    @Override
    public void compute(
        ComputeContext<LongWritable, MyValue, NullWritable, LongWritable> context,
        Iterable<LongWritable> msgs) throws IOException {

      // Messages sent to inactive vertex are ignored.
      if (!this.getValue().isActive()) {
        this.voteToHalt();
        return;
      }

      int stage = ((SCCAggrValue) context.getLastAggregatedValue(0)).getStage();
      switch (stage) {

        case STAGE_TRANSPOSE_1:
          context.sendMessageToNeighbors(this, this.getId());
          break;

        case STAGE_TRANSPOSE_2:
          for (LongWritable msg : msgs) {
            this.getValue().addInNeighbor(msg);
          }

        case STAGE_TRIMMING:
          this.getValue().setSccID(getId());
          if (this.getValue().getInNeighbors().size() == 0 ||
              this.getNumEdges() == 0) {
            this.getValue().setActive(false);
          }
          break;

        case STAGE_FW_START:
          this.getValue().setSccID(getId());
          context.sendMessageToNeighbors(this, this.getValue().getSccID());
          break;

        case STAGE_FW_REST:
          long minSccID = Long.MAX_VALUE;
          for (LongWritable msg : msgs) {
            if (msg.get() < minSccID) {
              minSccID = msg.get();
            }
          }

          if (minSccID < this.getValue().getSccID().get()) {
            this.getValue().setSccID(new LongWritable(minSccID));
            context.sendMessageToNeighbors(this, this.getValue().getSccID());
            this.getValue().setUpdated(true);
          } else {
            this.getValue().setUpdated(false);
          }

          break;

        case STAGE_BW_START:
          if (this.getId().equals(this.getValue().getSccID())) {
            for (Writable neighbor : this.getValue().getInNeighbors().getAll()) {
              context.sendMessage((LongWritable) neighbor, this.getValue().getSccID());
            }
            this.getValue().setActive(false);
          }
          break;

        case STAGE_BW_REST:
          this.getValue().setUpdated(false);
          for (LongWritable msg : msgs) {
            if (msg.equals(this.getValue().getSccID())) {
              for (Writable neighbor : this.getValue().getInNeighbors().getAll()) {
                context.sendMessage((LongWritable) neighbor, this.getValue().getSccID());
              }
              this.getValue().setActive(false);
              this.getValue().setUpdated(true);
              break;
            }
          }
          break;
      }

      context.aggregate(0, getValue());
    }

    @Override
    public void cleanup(
        WorkerContext<LongWritable, MyValue, NullWritable, LongWritable> context)
        throws IOException {
      context.write(getId(), getValue().getSccID());
    }
  }

  /**
   * The SCCAggrValue maintains global stage and graph updated and active status.
   * updated is true only if one vertex is updated.
   * active is true only if one vertex is active.
   */
  public static class SCCAggrValue implements Writable {

    IntWritable stage = new IntWritable(STAGE_TRANSPOSE_1);
    BooleanWritable updated = new BooleanWritable(false);
    BooleanWritable active = new BooleanWritable(false);

    public void setStage(int stage) {
      this.stage.set(stage);
    }

    public int getStage() {
      return this.stage.get();
    }

    public void setUpdated(boolean updated) {
      this.updated.set(updated);
    }

    public boolean getUpdated() {
      return this.updated.get();
    }

    public void setActive(boolean active) {
      this.active.set(active);
    }

    public boolean getActive() {
      return this.active.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.stage.write(out);
      this.updated.write(out);
      this.active.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.stage.readFields(in);
      this.updated.readFields(in);
      this.active.readFields(in);
    }

  }

  /**
   * The job of SCCAggregator is to schedule global stage in every superstep.
   */
  public static class SCCAggregator extends Aggregator<SCCAggrValue> {

    @SuppressWarnings("rawtypes")
    @Override
    public SCCAggrValue createStartupValue(WorkerContext context) throws IOException {
      return new SCCAggrValue();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public SCCAggrValue createInitialValue(WorkerContext context)
        throws IOException {
      return (SCCAggrValue) context.getLastAggregatedValue(0);
    }

    @Override
    public void aggregate(SCCAggrValue value, Object item) throws IOException {
      MyValue v = (MyValue) item;
      if ((value.getStage() == STAGE_FW_REST || value.getStage() == STAGE_BW_REST)
          && v.isUpdated()) {
        value.setUpdated(true);
      }

      // only active vertex invoke aggregate()
      value.setActive(true);
    }

    @Override
    public void merge(SCCAggrValue value, SCCAggrValue partial)
        throws IOException {
      boolean updated = value.getUpdated() || partial.getUpdated();
      value.setUpdated(updated);

      boolean active = value.getActive() || partial.getActive();
      value.setActive(active);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean terminate(WorkerContext context, SCCAggrValue value)
        throws IOException {

      // If all vertices is inactive, job is over.
      if (!value.getActive()) {
        return true;
      }

      // state machine
      switch (value.getStage()) {
        case STAGE_TRANSPOSE_1:
          value.setStage(STAGE_TRANSPOSE_2);
          break;
        case STAGE_TRANSPOSE_2:
          value.setStage(STAGE_TRIMMING);
          break;
        case STAGE_TRIMMING:
          value.setStage(STAGE_FW_START);
          break;
        case STAGE_FW_START:
          value.setStage(STAGE_FW_REST);
          break;
        case STAGE_FW_REST:
          if (value.getUpdated()) {
            value.setStage(STAGE_FW_REST);
          } else {
            value.setStage(STAGE_BW_START);
          }
          break;
        case STAGE_BW_START:
          value.setStage(STAGE_BW_REST);
          break;
        case STAGE_BW_REST:
          if (value.getUpdated()) {
            value.setStage(STAGE_BW_REST);
          } else {
            value.setStage(STAGE_TRIMMING);
          }
          break;
      }

      value.setActive(false);
      value.setUpdated(false);

      return false;
    }

  }


  public static class SCCVertexReader extends
                                      GraphLoader<LongWritable, MyValue, NullWritable, LongWritable> {

    @Override
    public void load(
        LongWritable recordNum,
        WritableRecord record,
        MutationContext<LongWritable, MyValue, NullWritable, LongWritable> context)
        throws IOException {
      SCCVertex vertex = new SCCVertex();

      vertex.setId((LongWritable) record.get(0));

      String[] edges = record.get(1).toString().split(";");
      for (int i = 0; i < edges.length; i++) {
        try {
          long destID = Long.parseLong(edges[i]);
          vertex.addEdge(new LongWritable(destID), NullWritable.get());
        } catch (NumberFormatException nfe) {
          System.err.println("Ignore " + nfe);
        }
      }
      context.addVertexRequest(vertex);
    }

  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("Usage: <input> <output>");
      System.exit(-1);
    }

    GraphJob job = new GraphJob();
    job.setGraphLoaderClass(SCCVertexReader.class);
    job.setVertexClass(SCCVertex.class);
    job.setAggregatorClass(SCCAggregator.class);

    job.addInput(TableInfo.builder().tableName(args[0]).build());
    job.addOutput(TableInfo.builder().tableName(args[1]).build());
    long startTime = System.currentTimeMillis();
    job.run();
    System.out.println("Job Finished in "
                       + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

  }

  @Test
  public void test() throws Exception {
    WareHouse wareHouse = WareHouse.getInstance();
    String project = TestUtils.yzy2;
    String outputTable = "strongly_connected_components_out";

    TestUtils.setEnvironment(project);

    //If output table exists then delete data (will not delete schema)
    wareHouse.dropTableDataIfExists(project, outputTable, null);
    Assert.assertEquals(true, wareHouse.isTableEmpty(project, outputTable, null));

    new StronglyConnectedComponents().main(new String[]{"connected_components_in", outputTable});

    Assert.assertEquals(false, wareHouse.isTableEmpty(project, outputTable, null));

    //read output table data
    List<Object[]> result = wareHouse.readData(project, outputTable, null, null, ',');
    Assert.assertEquals(11, result.size());

    // Sampling inspection
    Object[] record = result.get(2);
    Assert.assertEquals(2, record.length);
    Assert.assertEquals(true, record[0] instanceof Long);
    Assert.assertEquals(true, record[1] instanceof Long);
    Assert.assertEquals(3L, record[0]);
    Assert.assertEquals(3L, record[1]);

  }

}
