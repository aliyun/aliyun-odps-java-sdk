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
import com.aliyun.odps.examples.graph.SSSP.MinLongCombiner;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.GraphJob;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.local.common.WareHouse;

/**
 * Compute the connected component membership of each vertex and output
 * each vertex which's value containing the smallest id in the connected
 * component containing that vertex.
 *
 * Algorithm: propagate the smallest vertex id along the edges to all
 * vertices of a connected component.
 */
public class ConnectedComponents {

  public static class CCVertex extends
                               Vertex<LongWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    public void compute(
        ComputeContext<LongWritable, LongWritable, NullWritable, LongWritable> context,
        Iterable<LongWritable> msgs) throws IOException {

      if (context.getSuperstep() == 0L) {
        this.setValue(getId());
        context.sendMessageToNeighbors(this, getValue());
        return;
      }

      long minID = Long.MAX_VALUE;
      for (LongWritable id : msgs) {
        if (id.get() < minID) {
          minID = id.get();
        }
      }

      if (minID < this.getValue().get()) {
        this.setValue(new LongWritable(minID));
        context.sendMessageToNeighbors(this, getValue());
      } else {
        this.voteToHalt();
      }
    }

    /**
     * Output Table Description:
     * +-----------------+----------------------------------------+
     * | Field | Type    | Comment                                |
     * +-----------------+----------------------------------------+
     * | v     | bigint  | vertex id                              |
     * | minID | bigint  | smallest id in the connected component |
     * +-----------------+----------------------------------------+
     */
    @Override
    public void cleanup(
        WorkerContext<LongWritable, LongWritable, NullWritable, LongWritable> context)
        throws IOException {
      context.write(getId(), getValue());
    }
  }


  /**
   * Input Table Description:
   * +-----------------+----------------------------------------------------+
   * | Field | Type    | Comment                                            |
   * +-----------------+----------------------------------------------------+
   * | v     | bigint  | vertex id                                          |
   * | es    | string  | comma separated target vertex id of outgoing edges |
   * +-----------------+----------------------------------------------------+
   *
   * Example:
   * For graph:
   * 1 ----- 2
   * |       |
   * 3 ----- 4
   * Input table:
   * +-----------+
   * | v  | es   |
   * +-----------+
   * | 1  | 2,3  |
   * | 2  | 1,4  |
   * | 3  | 1,4  |
   * | 4  | 2,3  |
   * +-----------+
   */
  public static class CCVertexReader extends
                                     GraphLoader<LongWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    public void load(
        LongWritable recordNum,
        WritableRecord record,
        MutationContext<LongWritable, LongWritable, NullWritable, LongWritable> context)
        throws IOException {
      CCVertex vertex = new CCVertex();

      vertex.setId((LongWritable) record.get(0));

      String[] edges = record.get(1).toString().split(";");
      for (int i = 0; i < edges.length; i++) {
        long destID = Long.parseLong(edges[i]);
        vertex.addEdge(new LongWritable(destID), NullWritable.get());
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
    job.setGraphLoaderClass(CCVertexReader.class);
    job.setVertexClass(CCVertex.class);
    job.setCombinerClass(MinLongCombiner.class);

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
    String outputTable = "connected_components_out";

    TestUtils.setEnvironment(project);

    //If output table exists then delete data (will not delete schema)
    wareHouse.dropTableDataIfExists(project, outputTable, null);
    Assert.assertEquals(true, wareHouse.isTableEmpty(project, outputTable, null));

    new ConnectedComponents().main(new String[]{"connected_components_in", outputTable});

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
    Assert.assertEquals(1L, record[1]);

  }
}
