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

import static org.junit.Assert.assertNotNull;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.example.WordCount;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.pipeline.Pipeline;

import apsara.odps.lot.Lot.LogicalOperatorTree;

public class LOTGeneratorTest {

  @Test
  public void testNoPipelineGenTree() {
    MetaExplorer metaExplorer = new MockMetaExplorer();
    JobConf job = new BridgeJobConf();

    job.setMapOutputKeySchema(SchemaUtils.fromString("isInside:boolean"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("pointCount:bigint"));

    TableInfo tableIn = TableInfo.builder().projectName("foo").tableName("mr_src").build();
    InputUtils.addTable(tableIn, job);
    TableInfo tableOut = TableInfo.builder().projectName("foo").tableName("mr_dst").build();
    OutputUtils.addTable(tableOut, job);
    ((BridgeJobConf) job).setInputSchema(tableIn, SchemaUtils.fromString("word:string"));
    ((BridgeJobConf) job).setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"),
        TableInfo.DEFAULT_LABEL);

    job.setMapperClass(WordCount.TokenizerMapper.class);
    job.setReducerClass(WordCount.SumReducer.class);
    job.setNumReduceTasks(1);

    LOTGenerator lotGen = new LOTGenerator(metaExplorer.getDefaultProject(), job, null);
    LogicalOperatorTree lot = lotGen.genTree();

    Assert.assertEquals(7L, lot.getOperatorsCount());
    Assert.assertEquals("DataSource_0", lot.getOperators(0).getDataSource().getId());
    Assert.assertEquals("MapTransform_1", lot.getOperators(1).getTransform().getId());
    Assert.assertEquals("DIS_2", lot.getOperators(2).getDistributeBy().getId());
    Assert.assertEquals("SORT_3", lot.getOperators(3).getSortBy().getId());
    Assert.assertEquals("ReduceTransform_4", lot.getOperators(4).getTransform().getId());
    Assert.assertEquals("SEL_5", lot.getOperators(5).getSelect().getId());
    Assert.assertEquals("DataSink_6", lot.getOperators(6).getDataSink().getId());
    Assert.assertEquals(apsara.odps.TypesProtos.Type.Bool, lot.getOperators(1).getTransform()
        .getSchema().getColumns(0).getType());
    Assert.assertEquals(apsara.odps.TypesProtos.Type.Integer, lot.getOperators(1).getTransform()
        .getSchema().getColumns(1).getType());
  }
  
  @Test
  public void testPipelineGenTree() {
    MetaExplorer metaExplorer = new MockMetaExplorer();
    JobConf job = new BridgeJobConf();
    Pipeline pipelineOld = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .addReducer(WordCount.SumReducer.class,
                    new Column[]{new Column("word", OdpsType.STRING)},
                    new Column[]{new Column("count", OdpsType.BIGINT)},
                    null, null, null, null, null)
        .addReducer(WordCount.SumReducer.class)
        .createPipeline();

    Pipeline.toJobConf(job, pipelineOld);

    TableInfo tableIn = TableInfo.builder().projectName("foo").tableName("mr_src").build();
    InputUtils.addTable(tableIn, job);
    TableInfo tableOut = TableInfo.builder().projectName("foo").tableName("mr_dst").build();
    OutputUtils.addTable(tableOut, job);
    ((BridgeJobConf) job).setInputSchema(tableIn, SchemaUtils.fromString("word:string"));
    ((BridgeJobConf) job).setOutputSchema(SchemaUtils.fromString("word:string,count:bigint"),
        TableInfo.DEFAULT_LABEL);

    Pipeline pipeline = Pipeline.fromJobConf(job);
    assertNotNull(pipeline);

    LOTGenerator lotGen = new LOTGenerator(metaExplorer.getDefaultProject(), job, pipeline);
    LogicalOperatorTree lot = lotGen.genTree();

    Assert.assertEquals(10L, lot.getOperatorsCount());
    Assert.assertEquals("DataSource_0", lot.getOperators(0).getDataSource().getId());
    Assert.assertEquals("MapTransform_1", lot.getOperators(1).getTransform().getId());
    Assert.assertEquals("DIS_2", lot.getOperators(2).getDistributeBy().getId());
    Assert.assertEquals("SORT_3", lot.getOperators(3).getSortBy().getId());
    Assert.assertEquals("ReduceTransform_4", lot.getOperators(4).getTransform().getId());
    Assert.assertEquals("DIS_5", lot.getOperators(5).getDistributeBy().getId());
    Assert.assertEquals("SORT_6", lot.getOperators(6).getSortBy().getId());
    Assert.assertEquals("ReduceTransform_7", lot.getOperators(7).getTransform().getId());
    Assert.assertEquals("SEL_8", lot.getOperators(8).getSelect().getId());
    Assert.assertEquals("DataSink_9", lot.getOperators(9).getDataSink().getId());
    Assert.assertEquals(apsara.odps.TypesProtos.Type.String, lot.getOperators(1).getTransform()
        .getSchema().getColumns(0).getType());
    Assert.assertEquals(apsara.odps.TypesProtos.Type.Integer, lot.getOperators(1).getTransform()
        .getSchema().getColumns(1).getType());
  }
}
