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

import java.io.FileInputStream;
import java.io.IOException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class CloudSortUploadExample {

  public static void main(String args[]) {
    if (args.length < 5 || args.length > 6) {
      System.err.println("usage: CloudSortUploadExample inputfile accessid accesskey project table [partition]");
      System.exit(1);
    }
    String inputFile = args[0];
    String accessId = args[1];
    String accessKey = args[2];
    String project = args[3];
    String table = args[4];
    String partition = args.length > 5 ? args[5] : null;

    Odps odps = new Odps(new AliyunAccount(accessId, accessKey));
    String endpoint = "http://service.odps.aliyun.com/api";
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(project);

    try {
      System.out.println(String.format("uploading %s ...", inputFile));
      FileInputStream fis = new FileInputStream(inputFile);
      byte[] buf = new byte[100];

      TableTunnel tunnel = new TableTunnel(odps);
      // create upload session for table
      UploadSession uploadSession
          = partition == null ?
            tunnel.createUploadSession(project, table) :
            tunnel.createUploadSession(project, table, new PartitionSpec(partition));

      // open record writer
      RecordWriter recordWriter = uploadSession.openRecordWriter(0);
      Record record = uploadSession.newRecord();

      int c, rc = 0;
      while ((c = fis.read(buf)) > 0) {
        if (c != 100) {
          throw new RuntimeException("record length error");
        }
        record.setString(0, buf);
        recordWriter.write(record);
        rc++;
        if (rc % 100 == 0) {
          System.out.print(".");
        }
      }

      // close writer
      recordWriter.close();
      // commit uploadSession, the upload finish
      uploadSession.commit(new Long[]{0L});
      System.out.println(String.format("\nupload success, %s record(s) uploaded.", rc));
    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
