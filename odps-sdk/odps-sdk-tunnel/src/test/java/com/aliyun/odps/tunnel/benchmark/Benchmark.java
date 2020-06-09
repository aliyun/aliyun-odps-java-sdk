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

package com.aliyun.odps.tunnel.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.util.Utils;

public class Benchmark {

  @Test
  public void benckmarkBufferedUploadBufferSize() throws Exception {
    for (int i = 1; i <= 16; i *= 2) {
      for (int threadCount = 1; threadCount <= 64; threadCount *= 2) {
        CompressOption
            option =
            new CompressOption(CompressOption.CompressAlgorithm.ODPS_SNAPPY, 1, 0);
        long bufferSize = 1024  * 1024 * i;  // 1M 2M 4M 8M 16M
        testBufferedWriterUpload(threadCount, 1024 * 512 / threadCount, option, bufferSize);
      }
    }
    System.out.println("done.");
  }

  @Test
  public void benckmarkBufferredUpload() throws Exception {
    ArrayList<CompressOption.CompressAlgorithm>
        algList =
        new ArrayList<CompressOption.CompressAlgorithm>();
    algList.add(CompressOption.CompressAlgorithm.ODPS_RAW);
    algList.add(CompressOption.CompressAlgorithm.ODPS_SNAPPY);
    algList.add(CompressOption.CompressAlgorithm.ODPS_ZLIB);

    for (CompressOption.CompressAlgorithm alg : algList) {
      for (int threadCount = 1; threadCount <= 64; threadCount *= 2) {
        CompressOption option = new CompressOption(alg, 1, 0);
        testBufferedWriterUpload(threadCount, 1024 * 256 / threadCount, option, 10*1024*1024);
      }
    }
    System.out.println("done.");
  }

  @Test
  public void benchmarkRecordUpload() throws Exception {
    ArrayList<CompressOption.CompressAlgorithm>
        algList =
        new ArrayList<CompressOption.CompressAlgorithm>();
    algList.add(CompressOption.CompressAlgorithm.ODPS_RAW);
    algList.add(CompressOption.CompressAlgorithm.ODPS_SNAPPY);
    algList.add(CompressOption.CompressAlgorithm.ODPS_ZLIB);

    for (CompressOption.CompressAlgorithm alg : algList) {
      for (int threadCount = 1; threadCount <= 64; threadCount *= 2) {
        CompressOption option = new CompressOption(alg, 1, 0);
        testRecordWriterUpload(threadCount, 1024 * 256 / threadCount, option);
      }
    }
    System.out.println("done.");
  }

  @Ignore
  @Test
  public void benchmarkDownload() {
    ArrayList<CompressOption.CompressAlgorithm>
        algList =
        new ArrayList<CompressOption.CompressAlgorithm>();
    algList.add(CompressOption.CompressAlgorithm.ODPS_RAW);
    algList.add(CompressOption.CompressAlgorithm.ODPS_SNAPPY);
    algList.add(CompressOption.CompressAlgorithm.ODPS_ZLIB);

    try {
      for (CompressOption.CompressAlgorithm alg : algList) {
        for (int threadCount = 1; threadCount <= 256; threadCount *= 2) {
          CompressOption option = new CompressOption(alg, 1, 0);
          testDownload(threadCount, 1024 * 64 / threadCount, option);
        }
      }

      System.out.println("done.");
    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


  @Ignore
  @Test
  public void MultiThreadUpload() throws OdpsException, IOException, InterruptedException {
    final String projectName = Utils.getProjectName();
    final String tableName = Utils.getRandomTableName();
    Utils.dropTableIfExists(tableName);
    Utils.exeSql("create table " + tableName + " (col1 string) partitioned by (pt string);");
    Utils.exeSql("alter table " + tableName + " add partition(pt='a');");
    TableTunnel tunnel = Utils.getTunnelInstance();
    Record record;
    TableTunnel.StreamUploadSession session = tunnel.createStreamUploadSession(projectName, tableName, new PartitionSpec("pt=a"));
    record = session.newRecord();
    record.setString("col1", "value");
    int threadNum = RandomUtils.nextInt(100) + 1;
    int packNum = RandomUtils.nextInt(100);
    int recordsPerPack = 1000000;
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < threadNum; ++t) {
      Record finalRecord = record;
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            TableTunnel.StreamRecordPack pack = session.newRecordPack();
            for (int i = 0; i < packNum; ++i) {
              for (int j = 0; j < recordsPerPack; ++j) {
                pack.append(finalRecord);
              }
              pack.flush();
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
      thread.start();
      threads.add(thread);
    }

    for (Thread thread : threads) {
      thread.join();
    }

    tunnel = Utils.getTunnelInstance();
    TableTunnel.DownloadSession down = tunnel.createDownloadSession(projectName, tableName, new PartitionSpec("pt=a"));
    System.out.println(down.getId());
    Assert.assertEquals(threadNum * packNum * recordsPerPack, down.getRecordCount());
    RecordReader reader = down.openRecordReader(0, down.getRecordCount());
    record = reader.read();
    int count = 0;
    while (record != null) {
      Assert.assertEquals("value", record.getString("col1"));
      ++count;
      record = reader.read();
    }
    Assert.assertEquals(threadNum * packNum * recordsPerPack, count);

    Utils.dropTableIfExists(tableName);
  }

  private void testBufferedWriterUpload(int threadCount, int recordCount, CompressOption option, long bufferSize)
      throws TunnelException, InterruptedException, IOException {
    final String projectName = Utils.getProjectName();
    final String tableName = Utils.getRandomTableName();
    Utils.dropTableIfExists(tableName);
    Utils.exeSql("create table " + tableName + " (col1 bigint, col2 double, col3 boolean,"
                 + " col4 string, col5 string, col6 datetime);");

    final TableTunnel tunnel = Utils.getTunnelInstance();
    TableTunnel.UploadSession up = tunnel.createUploadSession(projectName, tableName);
    BufferedWriterThread[] threads = new BufferedWriterThread[threadCount];

    for (int i = 0; i < threadCount; ++i) {
      threads[i] = new BufferedWriterThread(i, up, recordCount, option, bufferSize);
      threads[i].start();
    }

    for (int i = 0; i < threadCount; ++i) {
      threads[i].join();
    }
    up.commit();

    long elapsedTime = 0;
    long sumTime = 0;
    for (int i = 0; i < threadCount; ++i) {
      long threadTime = threads[i].getEndTime() - threads[i].getStartTime();
      elapsedTime = Math.max(threadTime, elapsedTime);
      sumTime += threadTime;
    }

    System.out.printf("%d,%d,%d,%d,%d,%d,%s\n", threadCount, elapsedTime, sumTime,
                      threads[0].getRecordCount(), threads[0].getByteCount(), bufferSize,
                      option.algorithm);

    Utils.dropTableIfExists(tableName);
  }

  private void testRecordWriterUpload(int threadCount, int recordCount, CompressOption option)
      throws TunnelException, InterruptedException, IOException {
    final String projectName = Utils.getProjectName();
    final String tableName = Utils.getRandomTableName();
    Utils.dropTableIfExists(tableName);
    Utils.exeSql("create table " + tableName + " (col1 bigint, col2 double, col3 boolean,"
                 + " col4 string, col5 string, col6 datetime);");

    final TableTunnel tunnel = Utils.getTunnelInstance();
    TableTunnel.UploadSession up = tunnel.createUploadSession(projectName, tableName);
    RecordWriterThread[] threads = new RecordWriterThread[threadCount];

    for (int i = 0; i < threadCount; ++i) {
      threads[i] = new RecordWriterThread(i, up, recordCount, option);
      threads[i].start();
    }

    for (int i = 0; i < threadCount; ++i) {
      threads[i].join();
    }
    up.commit();

    long elapsedTime = 0;
    long sumTime = 0;
    for (int i = 0; i < threadCount; ++i) {
      long threadTime = threads[i].getEndTime() - threads[i].getStartTime();
      elapsedTime = Math.max(threadTime, elapsedTime);
      sumTime += threadTime;
    }

    System.out.printf("%d,%d,%d,%d,%d,%s\n", threadCount, elapsedTime, sumTime,
                      threads[0].getRecordCount(), threads[0].getByteCount(), option.algorithm);
    Utils.dropTableIfExists(tableName);
  }

  private void testDownload(int threadCount, int recordCount, CompressOption option)
      throws TunnelException, InterruptedException {
    final String projectName = Utils.getProjectName();
    final String tableName = Utils.getRandomTableName();
    Utils.dropTableIfExists(tableName);
    Utils.exeSql("create table " + tableName + " (col1 bigint, col2 double, col3 boolean,"
                 + " col4 string, col5 string, col6 datetime);");

    TableTunnel tunnel = Utils.getTunnelInstance();
    TableTunnel.DownloadSession session = tunnel.createDownloadSession(projectName, tableName);
    DownloadThread[] threads = new DownloadThread[threadCount];

    long time = System.currentTimeMillis();
    for (int i = 0; i < threadCount; ++i) {
      threads[i] = new DownloadThread(i, session, i * recordCount, recordCount, option);
      threads[i].start();
    }
    for (int i = 0; i < threadCount; ++i) {
      threads[i].join();
    }
    time = System.currentTimeMillis() - time;
    for (int i = 0; i < threadCount; ++i) {
      System.out.println(
          "" + threads[i].getByteCount() + "," + threads[i].getStartTime() + "," + threads[i]
              .getEndTime() + "," + time + "," + threadCount + "," + recordCount + ","
          + option.algorithm);
    }

    Utils.dropTableIfExists(tableName);
  }
}
