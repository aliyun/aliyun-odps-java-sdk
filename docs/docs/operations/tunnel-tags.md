---
title: 对 Tunnel 操作打标
sidebar_position: 1
---

在**资源观测**场景下，可能会遇到对 Tunnel 操作打标的需求，用来后续统计 Tunnel 操作的数量和相关信息。本文档介绍如何对
Tunnel 操作打标。

## 打标方式

### Tunnel 相关操作

对于 Tunnel
相关操作，可以将需要配置的标记（Tags）配置在 [Configuration](../api-reference/tunnel/Configuration.md)
类中。
Configuration 类提供了 [withTags](../api-reference/tunnel/Configuration.md#设置标签列表)
方法，可以将需要配置的标记（Tags）配置在 Configuration 类中。

**使用示例**：

```java 
// ingore imports

public class TagsTest {

  private static final String ENDPOINT;
  private static final String ACCESS_ID;
  private static final String ACCESS_KEY;
  private static final String PROJECT;

  public static void main(String[] args) throws TunnelException, IOException {
    Account account = new AliyunAccount(ACCESS_ID, ACCESS_KEY);
    Odps odps = new Odps(account);
    odps.setEndpoint(ENDPOINT);
    odps.setDefaultProject(PROJECT);

    Configuration configuration =
        Configuration.builder(odps).withTags(ImmutableList.of("test_tag")).build();
    TableTunnel.DownloadSession
        downloadSession =
        odps.tableTunnel(configuration).buildDownloadSession(odps.getDefaultProject(), "testtable")
            .build();
    long recordCount = downloadSession.getRecordCount();
    TunnelRecordReader recordReader = downloadSession.openRecordReader(0, recordCount);
    recordReader.forEach(record -> {
      System.out.println(record);
    });
  }
}
```

### 开放存储相关操作
对于 Tunnel
相关操作，可以将需要配置的标记（Tags）配置在 EnvironmentSettings 类中。
EnvironmentSettings.Builder 同样提供了 [withTags] 方法，可以将需要配置的标记（Tags）配置在 EnvironmentSettings 类中。

**使用示例**：
```java
// ingore imports

public class Main {

  private static final String ENDPOINT;
  private static final String ACCESS_ID;
  private static final String ACCESS_KEY;
  private static final String PROJECT;

  public static void main(String[] args) throws IOException {
    Account account = new AliyunAccount(ACCESS_ID, ACCESS_KEY);
    Credentials
        credentials = Credentials.newBuilder().withAccount(account)
        .build();

    EnvironmentSettings settings =
        EnvironmentSettings.newBuilder().withCredentials(credentials)
            .withServiceEndpoint(ENDPOINT)
            .withTags(ImmutableList.of("test_tag"))
            .build();
    
    TableReadSessionBuilder builder = new TableReadSessionBuilder();
    TableBatchReadSession
        session =
        builder.withSettings(settings)
            .identifier(TableIdentifier.of(PROJECT, "tableName"))
            .buildBatchReadSession();
    InputSplitAssigner inputSplitAssigner = session.getInputSplitAssigner();
    InputSplit[] allSplits = inputSplitAssigner.getAllSplits();
    for (InputSplit split : allSplits) {
      SplitReader<VectorSchemaRoot>
          arrowReader =
          session.createArrowReader(split,
                                    ReaderOptions.newBuilder().withSettings(settings).build());

      while (arrowReader.hasNext()) {
        VectorSchemaRoot vectorSchemaRoot = arrowReader.get();
        System.out.println(vectorSchemaRoot.contentToTSVString());
      }
      arrowReader.close();
    }
  }
}
```
## 观测方式

对于 Tunnel Tags 的操作记录，经过约 5 分钟左右的处理，
将被保存在 [Information Schema](https://help.aliyun.com/zh/maxcompute/user-guide/tenant-level-information-schema)
中的 tunnels_history 表中。

可以通过下述 SQL 查询经过打标的操作记录

```sql
set odps.namespace.schema = true;
select * from  SYSTEM_CATALOG.INFORMATION_SCHEMA.TUNNELS_HISTORY where app_tags="test_tag";
```

:::info 
存在表 tunnels_history 中没有 app_tags 字段的场景，是因为该功能在您所在的区域还未开通，请耐心等待。
:::