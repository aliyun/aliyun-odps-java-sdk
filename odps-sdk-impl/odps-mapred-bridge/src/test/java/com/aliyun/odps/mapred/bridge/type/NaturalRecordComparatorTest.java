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

package com.aliyun.odps.mapred.bridge.type;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;

public class NaturalRecordComparatorTest {

  @Test
  public void testNull() {
    Column[] schema =
        new Column[]{
            new Column("col1", OdpsType.BIGINT),
            new Column("col2", OdpsType.BOOLEAN),
            new Column("col3", OdpsType.DOUBLE),
            new Column("col4", OdpsType.STRING)};
    NaturalRecordComparator comp = new NaturalRecordComparator(schema);

    assertTrue(comp.compare(new Object[]{null, null, null, null}, new Object[]{
        new LongWritable(0), new BooleanWritable(true), new DoubleWritable(0.0),
        new Text("")}) < 0);

    assertTrue(comp.compare(new Object[]{new LongWritable(0), new BooleanWritable(true),
                                         new DoubleWritable(0.0), new Text("")},
                            new Object[]{null, null, null, null}) > 0);

    assertTrue(comp.compare(new Object[]{null, null, null, null},
                            new Object[]{null, null, null, null}) == 0);
  }
}
