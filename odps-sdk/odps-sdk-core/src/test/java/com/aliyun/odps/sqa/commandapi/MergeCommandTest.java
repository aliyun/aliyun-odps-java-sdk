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

package com.aliyun.odps.sqa.commandapi;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.TypeInfoFactory;

public class MergeCommandTest extends TestBase {

  private static final String TEST_TABLE_NAME = "merge_task_test_table_name";

  @BeforeClass
  public static void createTable() throws OdpsException, InterruptedException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }

    TableSchema schema = new TableSchema();
    schema
        .addColumn(new Column("col1", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT)));
    schema.addColumn(new Column("col2",
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.BIGINT)));
    odps.tables().create(TEST_TABLE_NAME, schema);
  }

  @AfterClass
  public static void deleteTable() throws OdpsException {
    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }
  }

  private void uploadData() throws TunnelException, IOException {
    TableTunnel tunnel = new TableTunnel(odps);
    UploadSession uploadSession = tunnel
        .createUploadSession(odps.getDefaultProject(), TEST_TABLE_NAME);
    RecordWriter recordWriter = uploadSession.openRecordWriter(0);
    ArrayRecord record = (ArrayRecord) uploadSession.newRecord();
    List<Long> arrayData = Arrays.asList(1L, 2L, 3L);
    Map<String, Long> mapData = new HashMap<>();
    mapData.put("a", 1L);
    mapData.put("c", 2L);
    record.setArray(0, arrayData);
    record.setMap(1, mapData);
    recordWriter.write(record);
    recordWriter.close();
    uploadSession.commit(new Long[]{0L});
  }

  @Test
  public void mergeCommandTest() throws OdpsException, IOException {
    uploadData();
    uploadData();

    String archiveCommand = "alter table " + TEST_TABLE_NAME + " merge smallfiles;";
    long numBeforeMerge = odps.tables().get(TEST_TABLE_NAME).getFileNum();
    CommandTestUtil.runCommandAndGetResult(odps, archiveCommand);
    long numAfterMerge = odps.tables().get(TEST_TABLE_NAME).getFileNum();
    Assert.assertTrue(numAfterMerge < numBeforeMerge);
  }

}