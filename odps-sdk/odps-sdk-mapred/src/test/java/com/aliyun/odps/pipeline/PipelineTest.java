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

package com.aliyun.odps.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.example.WordCount;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;

public class PipelineTest {

  @Test
  public void testMapOnly() throws OdpsException {

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .createPipeline();

    assertEquals(pipeline.getNodeNum(), 1);
    assertEquals(pipeline.getFirstNode().getType(), "map");
    assertEquals(pipeline.getLastNode().getType(), "map");
    assertNull(pipeline.getFirstNode().getOutputKeySchema());
    assertNull(pipeline.getFirstNode().getOutputValueSchema());
  }

  @Test
  public void testMR1() throws OdpsException {

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .addReducer(WordCount.SumReducer.class)
        .createPipeline();

    assertEquals(pipeline.getNodeNum(), 2);
    assertEquals(pipeline.getFirstNode().getType(), "map");
    assertEquals(pipeline.getLastNode().getType(), "reduce");
    TransformNode mapNode = pipeline.getFirstNode();
    assertNotNull(mapNode.getOutputKeySchema());
    assertNotNull(mapNode.getOutputValueSchema());
    assertTrue(mapNode.getOutputKeySortColumns().length > 0);
    assertEquals(mapNode.getOutputKeySortColumns()[0],
                 SchemaUtils.getNames(mapNode.getOutputKeySchema())[0]);

    assertTrue(mapNode.getOutputGroupingColumns().length > 0);
    assertEquals(mapNode.getOutputGroupingColumns()[0],
                 SchemaUtils.getNames(mapNode.getOutputKeySchema())[0]);
  }

  @Test
  public void testMR2() throws OdpsException {

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class,
                   new Column[]{new Column("word", OdpsType.STRING)},
                   new Column[]{new Column("count", OdpsType.BIGINT)},
                   null, null, null, null, null)
        .addReducer(WordCount.SumReducer.class)
        .createPipeline();

    assertEquals(pipeline.getNodeNum(), 2);
    assertEquals(pipeline.getFirstNode().getType(), "map");
    assertEquals(pipeline.getLastNode().getType(), "reduce");
    TransformNode mapNode = pipeline.getFirstNode();
    assertNotNull(mapNode.getOutputKeySchema());
    assertNotNull(mapNode.getOutputValueSchema());
    assertTrue(mapNode.getOutputKeySortColumns().length > 0);
    assertEquals(mapNode.getOutputKeySortColumns()[0],
                 SchemaUtils.getNames(mapNode.getOutputKeySchema())[0]);

    assertTrue(mapNode.getOutputGroupingColumns().length > 0);
    assertEquals(mapNode.getOutputGroupingColumns()[0],
                 SchemaUtils.getNames(mapNode.getOutputKeySchema())[0]);
  }

  @Test
  public void testMRR() throws OdpsException {

    Pipeline pipelineOld = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .addReducer(WordCount.SumReducer.class,
                    new Column[]{new Column("word", OdpsType.STRING)},
                    new Column[]{new Column("count", OdpsType.BIGINT)},
                    null, null, null, null, null)
         .setNumTasks(10)
        .addReducer(WordCount.SumReducer.class)
        .createPipeline();
    JobConf conf = new JobConf();
    Pipeline.toJobConf(conf, pipelineOld);
    conf.setNumReduceTasks(2);

    Pipeline pipeline = Pipeline.fromJobConf(conf);
    assertNotNull(pipeline);

    assertEquals(pipeline.getNodeNum(), 3);
    assertEquals(pipeline.getFirstNode().getType(), "map");
    assertEquals(pipeline.getFirstNode().getNextNode().getType(), "reduce");
    assertEquals(pipeline.getLastNode().getType(), "reduce");
    TransformNode interNode = pipeline.getNode(1);
    assertNotNull(interNode.getOutputKeySchema());
    assertNotNull(interNode.getOutputValueSchema());
    assertTrue(interNode.getOutputKeySortColumns().length > 0);
    assertEquals(interNode.getOutputKeySortColumns()[0],
                 SchemaUtils.getNames(interNode.getOutputKeySchema())[0]);

    assertTrue(interNode.getOutputGroupingColumns().length > 0);
    assertEquals(interNode.getOutputGroupingColumns()[0],
                 SchemaUtils.getNames(interNode.getOutputKeySchema())[0]);
    assertEquals(interNode.getOutputValueSchema()[0].getName(), "count");
    assertEquals(interNode.getOutputValueSchema()[0].getType(), OdpsType.BIGINT);

    assertEquals(interNode.getNumTasks(), 10);
    assertEquals(interNode.getNextNode().getNumTasks(), 2);
  }
}
