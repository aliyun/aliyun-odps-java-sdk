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

package com.aliyun.odps.mapred.local.sandbox;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

public class SandboxNegetive {
  public static String basePath;

  public native static int add(int x, int y);

  public static class TokenizerMapper extends MapperBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      Assert.assertNotNull(System.getSecurityManager());

      // case 1 permission java.io.FilePermission "." ,"read,write,delete";
      boolean flag = false;
      try {
        File f = new File(".");
        if (f.exists()) {
          System.out.println("hello world");
        }
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);

      // case 2 permission java.io.FilePermission "." ,"read,write,delete";
      flag = false;
      try {
        URL url = new URL("http://www.baidu.com");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        InputStreamReader reader = new InputStreamReader(conn.getInputStream());
        BufferedReader br = new BufferedReader(reader);
        String line;
        while ((line = br.readLine()) != null) {
          System.out.println(line);
        }
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);

      // case 3 permission java.io.FilePermission "<<ALL FILES>>" ,"execute";
      flag = false;
      try {
        Runtime.getRuntime().exec("ls");
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);

      // case 4 permission java.lang.RuntimePermission "modifyThreadGroup";
      flag = false;
      try {
        new N().start();
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);

      // case 5 permission java.lang.RuntimePermission "createClassLoader";
      flag = false;
      try {
        URL[] urls = new URL[] {new URL("http://www.aliyun.com/")};
        new URLClassLoader(urls);
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);


      // case 6 permission java.net.SocketPermission "localhost:1024-", "listen,resolve";
      flag = false;
      try {
        DatagramSocket socket = new DatagramSocket();
        socket.getLocalAddress();
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);


      // case 7 permission java.lang.RuntimePermission "loadLibrary.test.so";"
      flag = false;
      try {
        System.load(basePath + "/libJniTest.so");
        Assert.assertEquals(3, JniTest.add(1, 2));
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);

      // case 8: permission java.lang.RuntimePermission "getProtectionDomain";"
      flag = false;
      try {
        System.loadLibrary("JniTest");
        Assert.assertEquals(3, JniTest.add(1, 2));
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);

      // case 9: permission java.lang.RuntimePermission "getProtectionDomain";"
      flag = false;
      try {

        System.out.println(SandboxPositive.class.getProtectionDomain().getCodeSource()
            .getLocation().getPath());
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);


      // case 10: permission java.lang.RuntimePermission "readSystemProperty";
      flag = false;
      try {
        System.out.println(System.getProperty("java.class.path"));
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);

      // case 11: permission java.lang.reflect.ReflectPermission "suppressAccessChecks";
      flag = false;
      try {
        Field field = SandboxPositive.class.getDeclaredField("privateVar");
        field.setAccessible(true);
        Assert.assertEquals("odps", field.get(null));
      } catch (Exception exception) {
        flag = true;
      }
      Assert.assertTrue(flag);
    }

  }

  public static class N extends Thread {
    public void run() {
      Assert.assertNotNull(System.getSecurityManager());
    }
  }


  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: wordcount <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);

    /////////////additional code/////////////////////
    job.setBoolean(Constants.LOCAL_SECURITY_ENABLE, true);
    /////////////////////////////////////////////////

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).partSpec("p1=1/p2=2").build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).partSpec("p1=1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }

  @Test
  public void test() throws Exception {
    basePath = SandboxPositive.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    basePath = basePath.substring(basePath.indexOf(":") + 1);

    WareHouse wareHouse = WareHouse.getInstance();
    String project = TestUtils.odps_test_mrtask;
    String outputTable = "sandbox_positive_wordcount_out";

    TestUtils.setEnvironment(project);

    new SandboxNegetive().main(new String[] {"l_ss", "l_p_ss", outputTable});

  }


}
