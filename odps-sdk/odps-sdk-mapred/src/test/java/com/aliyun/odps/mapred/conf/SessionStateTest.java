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

package com.aliyun.odps.mapred.conf;

import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Properties;

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;

public class SessionStateTest {

  @Test
  public void testMultiThread() throws InterruptedException {
    SessionState ss = SessionState.get();
    ss.getDefaultJob().set("foo", "bar");
    Thread thread = new Thread() {
      @Override
      public void run() {
        SessionState ss = SessionState.get();
        assertEquals("bar", ss.getDefaultJob().get("foo"));
      }
    };
    thread.start();
    thread.join();
  }

  @Test
  public void testRunningCluster() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    SessionState ss = SessionState.get();
    ss.setOdps(new Odps((Account)null));
    assertEquals(ss.getOdps().instances().getDefaultRunningCluster(), null);

    Properties properties = new Properties();
    URL resource = this.getClass().getClassLoader().getResource("console_conf.json");
    assertNotNull(resource);

    properties.setProperty("odps.exec.context.file", resource.getFile());

    Method loadContextFile = SessionState.class.getDeclaredMethod("loadContextFile", Properties.class);
    loadContextFile.setAccessible(true);
    loadContextFile.invoke(ss, properties);
    assertEquals(ss.getOdps().instances().getDefaultRunningCluster(), "test_cluster");
  }

  @Test
  public void testHttpsCheck() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    SessionState ss = SessionState.get();
    ss.setOdps(new Odps((Account)null));
    assertEquals(ss.getOdps().instances().getDefaultRunningCluster(), null);

    Properties properties = new Properties();
    URL resource = this.getClass().getClassLoader().getResource("console_conf.json");
    assertNotNull(resource);

    properties.setProperty("odps.exec.context.file", resource.getFile());

    Method loadContextFile = SessionState.class.getDeclaredMethod("loadContextFile", Properties.class);
    loadContextFile.setAccessible(true);
    loadContextFile.invoke(ss, properties);
    assertEquals(ss.getOdps().getRestClient().isIgnoreCerts(), false);
  }

  @Test
  public void testSetCommandText() throws InterruptedException {
    SessionState ss = SessionState.get();
    // with invisible char
    ss.setCommandText("jar -resources oyz_test.jar -Dds=\005 -classpath oyz_test.jar wc.WordCount wc_in wc_out;");
    assertEquals("jar -resources oyz_test.jar -Dds= -classpath oyz_test.jar wc.WordCount wc_in wc_out;", ss.getCommandText());

    // with visible char
    ss.setCommandText("jar -resources oyz_test.jar -Dds=$ -classpath oyz_test.jar wc.WordCount wc_in wc_out;");
    assertEquals("jar -resources oyz_test.jar -Dds=$ -classpath oyz_test.jar wc.WordCount wc_in wc_out;", ss.getCommandText());

    // normal
    ss.setCommandText("jar -resources oyz_test.jar -classpath oyz_test.jar wc.WordCount wc_in wc_out;");
    assertEquals("jar -resources oyz_test.jar -classpath oyz_test.jar wc.WordCount wc_in wc_out;", ss.getCommandText());
  }

}
