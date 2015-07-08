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
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.GraphJob;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.local.common.WareHouse;

public class BipartiteMatching {

  private static final Text UNMATCHED = new Text("UNMATCHED");

  public static class TextPair implements Writable {

    public Text first;
    public Text second;

    public TextPair() {
      first = new Text();
      second = new Text();
    }

    public TextPair(Text first, Text second) {
      this.first = new Text(first);
      this.second = new Text(second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      first.write(out);
      second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      first = new Text();
      first.readFields(in);
      second = new Text();
      second.readFields(in);
    }

    @Override
    public String toString() {
      return first + ": " + second;
    }

  }

  public static class BipartiteMatchingVertexReader extends
                                                    GraphLoader<Text, TextPair, NullWritable, Text> {

    @Override
    public void load(LongWritable recordNum, WritableRecord record,
                     MutationContext<Text, TextPair, NullWritable, Text> context)
        throws IOException {
      BipartiteMatchingVertex vertex = new BipartiteMatchingVertex();
      vertex.setId((Text) record.get(0));
      vertex.setValue(new TextPair(UNMATCHED, (Text) record.get(1)));

      String[] adjs = record.get(2).toString().split(";");
      for (String adj : adjs) {
        vertex.addEdge(new Text(adj), null);
      }
      context.addVertexRequest(vertex);
    }

  }

  public static class BipartiteMatchingVertex extends
                                              Vertex<Text, TextPair, NullWritable, Text> {

    private static final Text LEFT = new Text("LEFT");
    private static final Text RIGHT = new Text("RIGHT");

    private static Random rand = new Random();

    @Override
    public void compute(
        ComputeContext<Text, TextPair, NullWritable, Text> context,
        Iterable<Text> messages) throws IOException {
      if (isMatched()) {
        voteToHalt();
        return;
      }

      switch ((int) context.getSuperstep() % 4) {
        case 0:
          if (isLeft()) {
            context.sendMessageToNeighbors(this, getId());
          }
          break;
        case 1:
          if (isRight()) {
            Text luckyLeft = null;
            for (Text message : messages) {
              if (luckyLeft == null) {
                luckyLeft = new Text(message);
              } else {
                if (rand.nextInt(1) == 0) {
                  luckyLeft.set(message);
                }
              }
            }
            if (luckyLeft != null) {
              context.sendMessage(luckyLeft, getId());
            }
          }
          break;
        case 2:
          if (isLeft()) {
            Text luckyRight = null;
            for (Text msg : messages) {
              if (luckyRight == null) {
                luckyRight = new Text(msg);
              } else {
                if (rand.nextInt(1) == 0) {
                  luckyRight.set(msg);
                }
              }
            }
            if (luckyRight != null) {
              setMatchVertex(luckyRight);
              context.sendMessage(luckyRight, getId());
            }
          }
          break;
        case 3:
          if (isRight()) {
            for (Text msg : messages) {
              setMatchVertex(msg);
            }
          }
          break;
      }
    }

    @Override
    public void cleanup(
        WorkerContext<Text, TextPair, NullWritable, Text> context)
        throws IOException {
      context.write(getId(), getValue().first);
    }

    private boolean isMatched() {
      return !getValue().first.equals(UNMATCHED);
    }

    private boolean isLeft() {
      return getValue().second.equals(LEFT);
    }

    private boolean isRight() {
      return getValue().second.equals(RIGHT);
    }

    private void setMatchVertex(Text matchVertex) {
      getValue().first.set(matchVertex);
    }
  }

  private static void printUsage() {
    System.err.println("BipartiteMatching <input> <output> [maxIteration]");
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      printUsage();
    }

    GraphJob job = new GraphJob();

    job.setGraphLoaderClass(BipartiteMatchingVertexReader.class);
    job.setVertexClass(BipartiteMatchingVertex.class);

    job.addInput(TableInfo.builder().tableName(args[0]).build());
    job.addOutput(TableInfo.builder().tableName(args[1]).build());
    int maxIteration = 30;
    if (args.length > 2) {
      maxIteration = Integer.parseInt(args[2]);
    }
    job.setMaxIteration(maxIteration);

    job.run();
  }

  @Test
  public void test() throws Exception {
    WareHouse wareHouse = WareHouse.getInstance();
    String project = TestUtils.yzy2;
    String outputTable = "bipartite_matching_out";

    TestUtils.setEnvironment(project);

    //If output table exists then delete data (will not delete schema)
    wareHouse.dropTableDataIfExists(project, outputTable, null);
    Assert.assertEquals(true, wareHouse.isTableEmpty(project, outputTable, null));

    new BipartiteMatching().main(new String[]{"bipartite_matching_in", outputTable});

    Assert.assertEquals(false, wareHouse.isTableEmpty(project, outputTable, null));

    //read output table data
    List<Object[]> result = wareHouse.readData(project, outputTable, null, null, ',');
    Assert.assertEquals(6, result.size());

    // Sampling inspection
    Object[] record = result.get(2);
    Assert.assertEquals(2, record.length);
    Assert.assertEquals(true, record[0] instanceof String);
    Assert.assertEquals(true, record[1] instanceof String);
    Assert.assertEquals("2", record[0]);
    Assert.assertEquals("1", record[1]);

  }

}
