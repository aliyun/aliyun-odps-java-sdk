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

package com.aliyun.odps.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.conf.JobConf.SortOrder;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class JobConfTest {

  @Test
  public void testClass() {
    JobConf conf = new JobConf();
    conf.setMapperClass(MapperBase.class);
    assertEquals(MapperBase.class.getName(), conf.getMapperClass().getName());
  }

  @Test
  public void testLoad() throws IOException {
    JobConf conf = new JobConf();
    conf.addResource("jobconf.xml");
    assertEquals(MapperBase.class.getName(), conf.getMapperClass().getName());
    InputStream is = ClassLoader.getSystemResourceAsStream("jobconf.xml");
    if (is == null) {
      fail("Job config jobconf.xml not found!");
    }
    conf = new JobConf();
    conf.addResource(is);
    assertEquals(MapperBase.class.getName(), conf.getMapperClass().getName());
  }
  
  @Test
  public void testLoadSessionContext() {
    JobConf defaultConf = new JobConf();
    defaultConf.set("default_key", "default_value");
    SessionState.get().setDefaultJob(defaultConf);
    
    JobConf conf = new JobConf();
    assertEquals(conf.get("default_key"), "default_value");
    Configuration mconf = new Configuration();
    
    mconf.set("test_key", "test_value");
    conf = new JobConf(mconf);
    assertEquals(conf.get("default_key"), "default_value");
    assertEquals(conf.get("test_key"), "test_value");
    
    conf = new JobConf(true);
    assertEquals(conf.get("default_key"), "default_value");
    conf = new JobConf(false);
    assertNull(conf.get("default_key"));
    
    conf = new JobConf("jobconf.xml");
    assertEquals(conf.get("default_key"), "default_value");
    assertEquals(MapperBase.class.getName(), conf.getMapperClass().getName());
  }

  @Test
  public void testEmptyArray() {
    JobConf conf = new JobConf();
    conf.setOutputKeySortColumns(new String[0]);
    Assert.assertArrayEquals(new String[0], conf.getOutputKeySortColumns());
  }

  @Test(expected = NullPointerException.class)
  public void testSetNull() {
    JobConf conf = new JobConf();
    conf.set("foo", null);
  }

  @Test
  public void testSortOrder() {
    JobConf conf = new JobConf();
    conf.setMapOutputKeySchema(SchemaUtils.fromString("key:bigint,value:string"));
    Assert.assertArrayEquals(new SortOrder[]{SortOrder.ASC, SortOrder.ASC},
                             conf.getOutputKeySortOrder());
    conf.setOutputKeySortOrder(new SortOrder[]{SortOrder.DESC});
    Assert.assertArrayEquals(new SortOrder[]{SortOrder.DESC},
                             conf.getOutputKeySortOrder());
  }

  @Test
  public void testSchema() {
    JobConf conf = new JobConf();
    conf.setMapOutputKeySchema(SchemaUtils.fromString("isInside:boolean"));
    Column[] schema = conf.getMapOutputKeySchema();
    Assert.assertEquals("isInside", schema[0].getName());
    Assert.assertEquals(OdpsType.BOOLEAN, schema[0].getType());
  }

  @Test
  public void testDeprecation() {
    JobConf conf = new JobConf();
    conf.setOutputSchema(SchemaUtils.fromString("isInside:boolean"), "foo");
    Assert.assertEquals("true", conf.get(
        "odps.deprecated.com.aliyun.odps.mapred.conf.JobConf.setOutputSchema"));
  }
}
