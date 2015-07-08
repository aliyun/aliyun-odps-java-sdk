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

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.GraphJob;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.local.common.WareHouse;

/**
 * Set program arguments:
 * pagerank_in pagerank_out
 */
public class PageRank {

  public static class PageRankVertex extends
                                     Vertex<Text, DoubleWritable, NullWritable, DoubleWritable> {

    @Override
    public void compute(ComputeContext<Text, DoubleWritable, NullWritable, DoubleWritable> context,
                        Iterable<DoubleWritable> messages) throws IOException {
      if (context.getSuperstep() == 0) {
        setValue(new DoubleWritable(1.0 / context.getTotalNumVertices()));
      } else if (context.getSuperstep() >= 1) {
        double sum = 0;
        for (DoubleWritable msg : messages) {
          sum += msg.get();
        }
        DoubleWritable vertexValue = new DoubleWritable((0.15f / context.getTotalNumVertices())
                                                        + 0.85f * sum);
        setValue(vertexValue);
      }
      if (hasEdges()) {
        context.sendMessageToNeighbors(this, new DoubleWritable(getValue().get()
                                                                / getEdges().size()));
      }
    }

    @Override
    public void cleanup(WorkerContext<Text, DoubleWritable, NullWritable, DoubleWritable> context)
        throws IOException {
      context.write(getId(), getValue());
    }
  }

  public static class PageRankVertexReader extends
                                           GraphLoader<Text, DoubleWritable, NullWritable, DoubleWritable> {

    @Override
    public void load(LongWritable recordNum, WritableRecord record,
                     MutationContext<Text, DoubleWritable, NullWritable, DoubleWritable> context)
        throws IOException {
      PageRankVertex vertex = new PageRankVertex();
      vertex.setValue(new DoubleWritable(0));
      vertex.setId((Text) record.get(0));
      System.out.println(record.get(0));

      for (int i = 1; i < record.size(); i++) {
        Writable edge = record.get(i);
        System.out.println(edge.toString());
        if (!(edge.equals(NullWritable.get()))) {
          vertex.addEdge(new Text(edge.toString()), NullWritable.get());
        }
      }
      System.out.println("vertex edgs size: " + (vertex.hasEdges() ? vertex.getEdges().size() : 0));
      context.addVertexRequest(vertex);
    }

  }

  private static void printUsage() {
    System.out.println("Usage: <in> <out> [Max iterations (default 30)]");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      printUsage();
    }

    GraphJob job = new GraphJob();

    job.setGraphLoaderClass(PageRankVertexReader.class);
    job.setVertexClass(PageRankVertex.class);
    job.addInput(TableInfo.builder().tableName(args[0]).build());
    job.addOutput(TableInfo.builder().tableName(args[1]).build());

    // default max iteration is 30
    job.setMaxIteration(30);
    if (args.length >= 3) {
      job.setMaxIteration(Integer.parseInt(args[2]));
    }

    long startTime = System.currentTimeMillis();
    job.run();
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
                       + " seconds");
  }

  @Test
  public void test() throws Exception {
    WareHouse wareHouse = WareHouse.getInstance();
    String project = TestUtils.yzy2;
    String outputTable = "pagerank_out";

    TestUtils.setEnvironment(project);

    //If output table exists then delete data (will not delete schema)
    wareHouse.dropTableDataIfExists(project, outputTable, null);
    Assert.assertEquals(true, wareHouse.isTableEmpty(project, outputTable, null));

    new PageRank().main(new String[]{"pagerank_in", outputTable});

    Assert.assertEquals(false, wareHouse.isTableEmpty(project, outputTable, null));

    //read output table data
    List<Object[]> result = wareHouse.readData(project, outputTable, null, null, ',');
    Assert.assertEquals(4, result.size());

    // Sampling inspection
    Object[] record = result.get(2);
    Assert.assertEquals(2, record.length);
    Assert.assertEquals(true, record[0] instanceof String);
    Assert.assertEquals(true, record[1] instanceof Double);
    Assert.assertEquals("4", record[0]);
    Assert.assertEquals(0.155702636559485D, record[1]);

  }
}
