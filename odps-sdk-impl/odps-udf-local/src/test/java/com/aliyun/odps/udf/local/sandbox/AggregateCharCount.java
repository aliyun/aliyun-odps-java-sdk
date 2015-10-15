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

package com.aliyun.odps.udf.local.sandbox;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.security.SecurityClient;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.runner.AggregatorRunner;
import com.aliyun.odps.udf.local.runner.BaseRunner;

public class AggregateCharCount extends Aggregator {
  public static String basePath;

  public native static int add(int x, int y);

  static Odps odps;

  @Before
  public void before() {
    Account account = new AliyunAccount("accessId", "accessKey");
    odps = new Odps(account);
    odps.setEndpoint("endpoint");
    odps.setDefaultProject("project_name");

    basePath = WareHouse.getInstance().getResourceDir(odps.getDefaultProject()).getAbsolutePath();

  }

  @Override
  public void setup(ExecutionContext ctx) throws UDFException {

    Assert.assertNotNull(System.getSecurityManager());

    // case 1 permission java.io.FilePermission "." ,"read,write,delete";
    File f = new File(".");
    if (f.exists()) {
      System.out.println("OK");
    }

    // case 2 permission java.net.SocketPermission "www.aliyun.com:80", "connect,resolve";
    URL url;
    try {
      url = new URL("http://www.aliyun.com");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      InputStreamReader reader = new InputStreamReader(conn.getInputStream());
      BufferedReader br = new BufferedReader(reader);
      br.close();
    } catch (Exception e1) {
      throw new UDFException(e1);
    }


    // case 3 permission java.io.FilePermission "<<ALL FILES>>" ,"execute";
    try {
      Runtime.getRuntime().exec("pwd");
    } catch (IOException e1) {
      throw new UDFException(e1);
    }

    // case 4 permission java.lang.RuntimePermission "modifyThreadGroup";
    new N().start();

    // case 5 permission java.lang.RuntimePermission "createClassLoader";

    try {
      URL[] urls = new URL[] {new URL("http://www.aliyun.com/")};
      new URLClassLoader(urls);
    } catch (MalformedURLException e1) {
      throw new UDFException(e1);
    }


    // case 6 permission java.net.SocketPermission "localhost:1024-", "listen,resolve";
    try {
      DatagramSocket socket = new DatagramSocket();
      socket.getLocalAddress();
    } catch (SocketException e1) {
      throw new UDFException(e1);
    }


    // case 7: permission java.lang.RuntimePermission "loadLibrary.test.so";
    System.load(basePath + "/libJniTest.so");
    Assert.assertEquals(3, JniTest.add(1, 2));

    // case 8: permission java.lang.RuntimePermission "loadLibrary.test.so";
    System.loadLibrary("JniTest");
    Assert.assertEquals(3, JniTest.add(1, 2));

    // case 9: permission java.lang.RuntimePermission "getProtectionDomain";
    System.out.println(UdfExample.class.getProtectionDomain().getCodeSource().getLocation()
        .getPath());

    // case 10: permission java.lang.RuntimePermission "readSystemProperty";
    System.out.println(System.getProperty("java.class.path"));

    // case 11: permission java.lang.reflect.ReflectPermission "suppressAccessChecks";
    try {
      Field field = UdfExample.class.getDeclaredField("privateVar");
      field.setAccessible(true);
      Assert.assertEquals("odps", field.get(null));
    } catch (Exception e) {
      throw new UDFException(e);
    }

  }

  @Override
  public void iterate(Writable arg0, Writable[] arg1) throws UDFException {
    LongWritable result = (LongWritable) arg0;
    for (Writable item : arg1) {
      Text txt = (Text) item;
      result.set(result.get() + txt.getLength());
    }

  }

  @Override
  public void merge(Writable arg0, Writable arg1) throws UDFException {
    LongWritable result = (LongWritable) arg0;
    LongWritable partial = (LongWritable) arg1;
    result.set(result.get() + partial.get());

  }

  @Override
  public Writable newBuffer() {
    return new LongWritable(0L);
  }

  @Override
  public Writable terminate(Writable arg0) throws UDFException {
    return arg0;
  }


  public static class N extends Thread {
    public void run() {
      File f = new File(".");
      if (f.exists()) {
        System.out.println("OK");
      }
      Assert.assertNotNull(System.getSecurityManager());
    }
  }
  
  @Test
  public void test() throws LocalRunException, UDFException {
    SecurityClient.clear();

    // open security switch
    System.setProperty(Constants.LOCAL_SECURITY_ENABLE, "true");

    StringBuffer policy = new StringBuffer();
    policy.append("permission java.io.FilePermission  \".\" , \"read,write,delete\";");
    policy
        .append("permission java.net.SocketPermission \"www.aliyun.com:80\", \"connect,resolve\";");
    policy.append("permission  java.io.FilePermission  \"<<ALL FILES>>\" ,\"execute\";");
    policy.append("permission java.lang.RuntimePermission \"modifyThreadGroup\";");
    policy.append("permission java.net.SocketPermission \"localhost:1024-\", \"listen,resolve\";");
    policy.append("permission java.lang.RuntimePermission \"createClassLoader\";");
    policy.append("permission java.lang.RuntimePermission \"getProtectionDomain\";");
    policy.append("permission java.lang.RuntimePermission \"readSystemProperty\";");
    policy.append("permission java.lang.reflect.ReflectPermission \"suppressAccessChecks\";");

    System.setProperty(Constants.LOCAL_SECURITY_JNI_ENABLE, "true");

    System.setProperty(Constants.LOCAL_USER_DEFINE_POLICY, policy.toString());

    // runner = new AggregatorRunner(null, "com.aliyun.odps.udf.local.examples.AggregateCharCount");
    BaseRunner runner = new AggregatorRunner(odps, new AggregateCharCount());
    runner.feed(new Object[] {"one", "one"}).feed(new Object[] {"three", "three"})
        .feed(new Object[] {"four", "four"});
    List<Object[]> out = runner.yield();
    Assert.assertEquals(1, out.size());
    Assert.assertEquals(24L, out.get(0)[0]);

  }



}
