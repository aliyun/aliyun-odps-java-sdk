---
title: 使用 Tunnel 进行流式上传
sidebar_label: 使用 Tunnel 进行流式上传
sidebar_position: 4
---

# 阿里云 MaxCompute (ODPS) 数据流上传示例

本文档展示了如何使用阿里云MaxCompute的流式上传功能将数据上传到ODPS表中。示例使用Access Key进行认证。

## 目录

1. [简介](#简介)
2. [初始化ODPS客户端](#初始化ODPS客户端)

## 简介

`TunnelStreamingUploadExample`类展示了如何使用流式上传功能将数据上传到ODPS。请确保将占位符替换为您实际的ODPS凭证和参数。

### 示例代码

```java
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

public class TunnelStreamingUploadExample {
  private static String accessId = "<your access id>";
  private static String accessKey = "<your access Key>";
  private static String odpsUrl = "<your odps endpoint>";
  private static String project = "<your project>";
  private static String table = "<your table name>";
  private static String partition;

  private static Odps getOdps() {
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setEndpoint(odpsUrl);
    odps.setDefaultProject(project);
    return odps;
  }

  public static void main(String[] args) throws Exception {
    Odps odps = getOdps();
    createTestTable();
    try {
      TableTunnel.StreamUploadSession session =
          odps.tableTunnel()
              .buildStreamUploadSession(odps.getDefaultProject(), table)
              .setPartitionSpec(partition == null ? null : new PartitionSpec(partition))
              .build();
      TableTunnel.StreamRecordPack recordPack = session.newRecordPack();
      for (int i = 0; i < 3; i++) {
        Record record = session.newRecord();
        record.setBigint("c1", (long) i);
        record.setBigint("c2", (long) (i * 2));
        recordPack.append(record);
      }
      recordPack.flush();
      showTable();
      triggerSchemaEvolution();
      for (int retry = 0; retry < 10; retry++) {
        session =
            odps.tableTunnel()
                .buildStreamUploadSession(odps.getDefaultProject(), table)
                .setPartitionSpec(partition == null ? null : new PartitionSpec(partition))
                .build();
        if (session.getSchema().getAllColumns().size() == 3) {
          break;
        }
        TimeUnit.SECONDS.sleep(6);
      }
      recordPack = session.newRecordPack();
      for (int i = 0; i < 3; i++) {
        Record record = session.newRecord();
        record.setBigint("c1", (long) i);
        record.setBigint("c2", (long) (i * 2));
        record.setBigint("c3", (long) (i * 3));
        recordPack.append(record);
      }
      recordPack.flush();
      showTable();
    } catch (TunnelException e) {
      System.err.println("Tunnel Exception: " + e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println("IO Exception: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static void createTestTable() throws OdpsException {
    getOdps()
        .tables()
        .newTableCreator(table, TableSchema.builder()
            .withBigintColumn("c1")
            .withBigintColumn("c2")
            .build())
        .ifNotExists()
        .create();
  }

  private static void triggerSchemaEvolution() throws OdpsException {
    Instance instance = SQLTask.run(getOdps(), "alter table " + table + " add column c3 bigint;");
    instance.waitForSuccess();
  }

  private static void showTable() throws OdpsException {
    getOdps().tables().get(table).read(10).forEach(System.out::println);
    System.out.println("\n");
  }
}
```

## 初始化ODPS客户端

此方法使用Access Key进行认证，初始化ODPS客户端。

## 本文档待完善