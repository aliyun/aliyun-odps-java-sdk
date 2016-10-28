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

package com.aliyun.odps.mapred.bridge.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.mapred.bridge.MockMetaExplorer;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.example.WordCount;
import com.aliyun.odps.pipeline.Pipeline;

public class PipelineValidatorTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testMapOnly() throws OdpsException {
    JobConf conf = new JobConf();

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .createPipeline();
    Pipeline.toJobConf(conf, pipeline);

    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMRValid() throws OdpsException {

    JobConf conf = new JobConf();

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .addReducer(WordCount.SumReducer.class)
        .createPipeline();
    Pipeline.toJobConf(conf, pipeline);

    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMissKeySchema() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Operator ouput key schema not set");

    JobConf conf = new JobConf();

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .addReducer(WordCount.SumReducer.class)
        .createPipeline();
    Pipeline.toJobConf(conf, pipeline);

    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMissValueSchema() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Operator ouput value schema not set");

    JobConf conf = new JobConf();

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .addReducer(WordCount.SumReducer.class)
        .createPipeline();
    Pipeline.toJobConf(conf, pipeline);

    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMapAfterMap() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Operators after first node must be reducer");

    JobConf conf = new JobConf();

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .addMapper(WordCount.TokenizerMapper.class)
        .createPipeline();
    Pipeline.toJobConf(conf, pipeline);

    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMapFirst() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("First operator of pipeline must be mapper");

    JobConf conf = new JobConf();

    Pipeline pipeline = Pipeline.builder()
        .addReducer(WordCount.SumReducer.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .addMapper(WordCount.TokenizerMapper.class)
        .createPipeline();
    Pipeline.toJobConf(conf, pipeline);

    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testPartitionColumsInvalid() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Key partition columns should be inside of output key columns");

    JobConf conf = new JobConf();

    Pipeline pipeline = Pipeline.builder()
        .addMapper(WordCount.TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .setPartitionColumns(new String[]{"haha"})
        .addReducer(WordCount.SumReducer.class)
        .createPipeline();
    Pipeline.toJobConf(conf, pipeline);

    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }
}
