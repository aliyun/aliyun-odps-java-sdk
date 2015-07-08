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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.Aggregator;
import com.aliyun.odps.graph.Combiner;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.GraphJob;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.local.common.WareHouse;

public class TopologySort {

  private final static Log LOG = LogFactory.getLog(TopologySort.class);

  public static class TopologySortVertex extends
                                         Vertex<LongWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    public void compute(
        ComputeContext<LongWritable, LongWritable, NullWritable, LongWritable> context,
        Iterable<LongWritable> messages) throws IOException {
      // in superstep 0, each vertex sends message whose value is 1 to its
      // neighbors
      if (context.getSuperstep() == 0) {
        if (hasEdges()) {
          context.sendMessageToNeighbors(this, new LongWritable(1L));
        }
      } else if (context.getSuperstep() >= 1) {
        // compute each vertex's indegree
        long indegree = getValue().get();
        for (LongWritable msg : messages) {
          indegree += msg.get();
        }
        setValue(new LongWritable(indegree));
        if (indegree == 0) {
          voteToHalt();
          if (hasEdges()) {
            context.sendMessageToNeighbors(this, new LongWritable(-1L));
          }
          context.write(new LongWritable(context.getSuperstep()), getId());
          LOG.info("vertex: " + getId());
        }
        context.aggregate(new LongWritable(indegree));
      }
    }
  }

  public static class TopologySortVertexReader extends
                                               GraphLoader<LongWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    public void load(LongWritable recordNum, WritableRecord record,
                     MutationContext<LongWritable, LongWritable, NullWritable, LongWritable> context)
        throws IOException {
      TopologySortVertex vertex = new TopologySortVertex();
      vertex.setId((LongWritable) record.get(0));
      vertex.setValue(new LongWritable(0));

      String[] edges = record.get(1).toString().split(";");
      for (int i = 0; i < edges.length; i++) {
        long edge = Long.parseLong(edges[i]);
        if (edge >= 0) {
          vertex.addEdge(new LongWritable(Long.parseLong(edges[i])), NullWritable.get());
        }
      }
      LOG.info(record.toString());
      context.addVertexRequest(vertex);
    }

  }

  public static class LongSumCombiner extends Combiner<LongWritable, LongWritable> {

    @Override
    public void combine(LongWritable vertexId, LongWritable combinedMessage,
                        LongWritable messageToCombine) throws IOException {
      combinedMessage.set(combinedMessage.get() + messageToCombine.get());
    }

  }

  public static class TopologySortAggregator extends Aggregator<BooleanWritable> {

    @SuppressWarnings("rawtypes")
    @Override
    public BooleanWritable createInitialValue(WorkerContext context) throws IOException {
      return new BooleanWritable(true);
    }

    @Override
    public void aggregate(BooleanWritable value, Object item) throws IOException {
      boolean hasCycle = value.get();
      boolean inDegreeNotZero = ((LongWritable) item).get() == 0 ? false : true;
      value.set(hasCycle && inDegreeNotZero);
    }

    @Override
    public void merge(BooleanWritable value, BooleanWritable partial) throws IOException {
      value.set(value.get() && partial.get());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean terminate(WorkerContext context, BooleanWritable value) throws IOException {
      if (context.getSuperstep() == 0) {
        // since the initial aggregator value is true, and in superstep we don't
        // do aggregate
        return false;
      }
      return value.get();
    }

  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.out.println("Usage : <inputTable> <outputTable>");
      System.exit(-1);
    }

    // 杈撳叆琛ㄥ舰寮忎负
    // 0 1;2
    // 1 3
    // 2 3
    // 3 -1
    // 绗竴鍒椾负vertexid锛岀浜屽垪涓鸿鐐硅竟鐨刣estination vertexid锛岃嫢鍊间负-1锛岃〃绀鸿鐐规棤鍑鸿竟
    // 杈撳嚭琛ㄥ舰寮忎负
    // 1,0
    // 2,1
    // 2,2
    // 3,3
    // 绗竴鍒椾负supstep鍊硷紝闅愬惈浜嗘嫇鎵戦『搴忥紝绗簩鍒椾负vertexid
    // TopologySortAggregator鐢ㄦ潵鍒ゆ柇鍥句腑鏄惁鏈夌幆
    // 鑻ヨ緭鍏ョ殑鍥炬湁鐜紝鍒欏綋鍥句腑active鐨勭偣鍏ュ害閮戒笉锟�鏃讹紝杩唬缁撴潫
    // 鐢ㄦ埛鍙互閫氳繃杈撳叆琛ㄥ拰杈撳嚭琛ㄧ殑璁板綍鏁版潵鍒ゆ柇锟�锟斤拷鏈夊悜鍥炬槸鍚︽湁锟�
    GraphJob job = new GraphJob();
    job.setGraphLoaderClass(TopologySortVertexReader.class);
    job.setVertexClass(TopologySortVertex.class);
    job.addInput(TableInfo.builder().tableName(args[0]).build());
    job.addOutput(TableInfo.builder().tableName(args[1]).build());
    job.setCombinerClass(LongSumCombiner.class);
    job.setAggregatorClass(TopologySortAggregator.class);

    long startTime = System.currentTimeMillis();
    job.run();
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
                       + " seconds");
  }

  @Test
  public void test() throws Exception {
    WareHouse wareHouse = WareHouse.getInstance();
    String project = TestUtils.yzy2;
    String outputTable = "topology_sort_out";

    TestUtils.setEnvironment(project);

    // If output table exists then delete data (will not delete schema)
    wareHouse.dropTableDataIfExists(project, outputTable, null);
    Assert.assertEquals(true, wareHouse.isTableEmpty(project, outputTable, null));

    new TopologySort().main(new String[]{"topology_sort_in", outputTable});

    Assert.assertEquals(false, wareHouse.isTableEmpty(project, outputTable, null));

    // read output table data
    List<Object[]> result = wareHouse.readData(project, outputTable, null, null, ',');
    Assert.assertEquals(4, result.size());

    // Sampling inspection
    Object[] record = result.get(2);
    Assert.assertEquals(2, record.length);
    Assert.assertEquals(true, record[0] instanceof Long);
    Assert.assertEquals(true, record[1] instanceof Long);
    Assert.assertEquals(2L, record[0]);
    Assert.assertEquals(2L, record[1]);

  }
}
