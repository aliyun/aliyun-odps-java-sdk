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

package com.aliyun.odps.mapred.local.conf;

/*
 * 测试环境下可能已经初始化过SessionState，导致该测试用例失败,
 * 采用Eclipse插件启动（Run As Odps Application）时不存在这种情况
 */
public class LocalConfTest {

  // copy from com.aliyun.odps.mapred.conf.SessionState
  // Copied from old OdpsConf.
  private final static String OLD_PROJNAME_KEY = "odps.project.name";
  private final static String OLD_ENDPOINT_KEY = "odps.end.point";
  private final static String OLD_ACCESSID_KEY = "odps.access.id";
  private final static String OLD_ACCESSKEY_KEY = "odps.access.key";
  private final static String OLD_RUNMODE_KEY = "odps.runner.mode";
  private final static String OLD_RESOURCE_KEY = "odps.cache.resources";
  private final static String OLD_CP_RESOURCE_KEY = "odps.classpath.resources";
  private final static String OLD_CONTEXT_FILE = "odps.exec.context.file";

  private final static String OLD_ACCOUNT_PROVIDER_KEY = "odps.account.provider";
  private final static String OLD_TAOBAO_TOKEN_KEY = "odps.taobao.token";
  private final static String OLD_TAOBAO_ALGORITHM_KEY = "odps.taobao.algorithm";

  // Local MR
  private final static String LOCAL_TEMP_DIR = "odps.mapred.temp.dir";
  private final static String LOCAL_TEMP_RETAIN = "odps.mapred.temp.retain";

//  @Test
//  public void test() throws InterruptedException {
//    System.setProperty(OLD_PROJNAME_KEY, "project");
//    System.setProperty(OLD_ENDPOINT_KEY, "endpoint");
//    System.setProperty(OLD_ACCESSID_KEY, "accessId");
//    System.setProperty(OLD_ACCESSKEY_KEY, "accessKey");
//    System.setProperty(OLD_RUNMODE_KEY, "local");
//    System.setProperty(LOCAL_TEMP_DIR, "temp");
//    System.setProperty(LOCAL_TEMP_RETAIN, "false");
//
//    SessionState ss = SessionState.get();
//    Odps odps = ss.getOdps();
//    JobConf job = ss.getDefaultJob();
//    LocalConf localJob = new LocalConf(job);
//
//    ApsaraAccount account = (ApsaraAccount) odps.getAccount();
//    Assert.assertEquals(odps.getDefaultProject(), "project");
//    Assert.assertEquals(odps.getEndpoint(), "endpoint");
//    Assert.assertEquals(account.getAccessId(), "accessId");
//    Assert.assertEquals(account.getAccessKey(), "accessKey");
//
//    Assert.assertEquals(job.get(LOCAL_TEMP_DIR), "temp");
//    Assert.assertEquals(job.get(LOCAL_TEMP_RETAIN), "false");
//
//    Assert.assertEquals(localJob.getJobDir(), "temp");
//    Assert.assertEquals(localJob.isRetainTempData(), false);
//
//    Thread thread = new Thread() {
//      @Override
//      public void run() {
//        SessionState ss = SessionState.get();
//        Odps odps = ss.getOdps();
//        JobConf job = ss.getDefaultJob();
//        LocalConf localJob = new LocalConf(job);
//
//        ApsaraAccount account = (ApsaraAccount) odps.getAccount();
//        Assert.assertEquals(odps.getDefaultProject(), "project");
//        Assert.assertEquals(odps.getEndpoint(), "endpoint");
//        Assert.assertEquals(account.getAccessId(), "accessId");
//        Assert.assertEquals(account.getAccessKey(), "accessKey");
//
//        Assert.assertEquals(job.get(LOCAL_TEMP_DIR), "temp");
//        Assert.assertEquals(job.get(LOCAL_TEMP_RETAIN), "false");
//
//        Assert.assertEquals(localJob.getJobDir(), "temp");
//        Assert.assertEquals(localJob.isRetainTempData(), false);
//      }
//    };
//    thread.start();
//    thread.join();
//
//    job.set(LOCAL_TEMP_DIR, "temp1");
//    job.set(LOCAL_TEMP_RETAIN, "true");
//
//    Assert.assertEquals(job.get(LOCAL_TEMP_DIR), "temp1");
//    Assert.assertEquals(job.get(LOCAL_TEMP_RETAIN), "true");
//
//    localJob = new LocalConf(job);
//
//    Assert.assertEquals(localJob.getJobDir(), "temp1");
//    Assert.assertEquals(localJob.isRetainTempData(), true);
//
//    thread = new Thread() {
//      @Override
//      public void run() {
//        SessionState ss = SessionState.get();
//        JobConf job = ss.getDefaultJob();
//        LocalConf localJob = new LocalConf(job);
//
//        Assert.assertEquals(job.get(LOCAL_TEMP_DIR), "temp1");
//        Assert.assertEquals(job.get(LOCAL_TEMP_RETAIN), "true");
//
//        Assert.assertEquals(localJob.getJobDir(), "temp1");
//        Assert.assertEquals(localJob.isRetainTempData(), true);
//      }
//    };
//    thread.start();
//    thread.join();
//
//  }

}
