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
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.TableInfo.TableInfoBuilder;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.bridge.MockMetaExplorer;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.example.WordCount;
import com.aliyun.odps.mapred.utils.InputUtils;

public class ValidatorTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testValid() throws OdpsException {
    JobConf conf = new JobConf();
    conf.setNumReduceTasks(0);
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testNoMapper() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Mapper class not specified.");

    JobConf conf = new JobConf();
    conf.setNumReduceTasks(0);
    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testNoReducer() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Reducer class not specified.");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMalformedSchema() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Malformed map output key schema");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setMapOutputKeySchema(new Column[]{null});
    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMalformedSchema2() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Malformed map output key schema");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setMapOutputKeySchema(new Column[0]);
    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testTooManyTables() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Expecting no more than 1024 partitions");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);
    TableInfoBuilder builder = TableInfo.builder().projectName("foo").tableName("bar");
    for (int i = 0; i < 1025; i++) {
      InputUtils.addTable(builder.partSpec("ds=" + i).build(), conf);
    }
    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testTooManyTables2() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Expecting no more than 64 distinct tables");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);
    TableInfoBuilder builder = TableInfo.builder().projectName("foo");
    for (int i = 0; i < 65; i++) {
      InputUtils.addTable(builder.tableName("tbl" + i).build(), conf);
    }
    Validator validator = ValidatorFactory.getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testVolumePositive() throws OdpsException {
    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);

    InputUtils.addVolume(new VolumeInfo("bar", "foobar", "foobar"), conf);
    Validator validator = ValidatorFactory
        .getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testTooManyInputVolumes() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Too many input volumes:Expecting no more than 1024 partitions. ");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);
    for (int i = 0; i < 1025; i++) {
      InputUtils.addVolume(new VolumeInfo("bar", "ds" + i, "label" + i), conf);
    }
    Validator validator = ValidatorFactory
        .getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testTooManyInputVolumes2() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Too many input volumes:Expecting no more than 1024 partitions. ");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);

    for (int i = 0; i < 1025; i++) {
      InputUtils.addVolume(new VolumeInfo("bar" + i, "ds", "label" + i), conf);
    }
    Validator validator = ValidatorFactory
        .getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testDuplicatedVolumeLabel() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Multiple volumes share the same label:label");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);
    for (int i = 0; i < 2; i++) {
      InputUtils.addVolume(new VolumeInfo("bar", "ds" + i, "label"), conf);
    }
    Validator validator = ValidatorFactory
        .getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testDuplicatedVolume() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage("Duplicated volume definition found:bar.ds");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);
    for (int i = 0; i < 2; i++) {
      InputUtils.addVolume(new VolumeInfo("bar", "ds", "label" + i), conf);
    }
    Validator validator = ValidatorFactory
        .getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMalformedVolumeSpec() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage(
        "Malformed volume spec:illegal label 1, expecting [A-Z,a-z,0-9,_,#,\\.,\\-]* with length of [3,32]");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);
    InputUtils.addVolume(new VolumeInfo("bar", "ds", "1"), conf);
    Validator validator = ValidatorFactory
        .getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMalformedVolumeSpec1() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage(
        "Malformed volume spec:illegal label hello?, expecting [A-Z,a-z,0-9,_,#,\\.,\\-]* with length of [3,32]");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);
    InputUtils.addVolume(new VolumeInfo("bar", "ds", "hello?"), conf);
    Validator validator = ValidatorFactory
        .getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }

  @Test
  public void testMalformedVolumeSpec3() throws OdpsException {
    exception.expect(OdpsException.class);
    exception.expectMessage(
        "Malformed volume spec:illegal label -------------------------------------------, expecting [A-Z,a-z,0-9,_,#,\\.,\\-]* with length of [3,32]");

    JobConf conf = new JobConf();
    conf.setMapperClass(WordCount.TokenizerMapper.class);
    conf.setReducerClass(WordCount.SumReducer.class);
    conf.setNumReduceTasks(0);
    InputUtils.addVolume(new VolumeInfo("bar", "ds", "-------------------------------------------"),
                         conf);
    Validator validator = ValidatorFactory
        .getValidator(conf, new MockMetaExplorer());
    validator.validate();
  }
}
