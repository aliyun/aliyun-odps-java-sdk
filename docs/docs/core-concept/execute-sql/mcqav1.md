---
title: MCQA
sidebar_label: MCQA
sidebar_position: 2
---

:::note
了解 MCQA
的特性、架构和使用方式，请参考[阿里云官网文档](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-query-acceleration)
:::

## 核心接口

### Session

Session 类是用于管理和执行 SQL 查询会话的核心类，其代表一个在MaxCompute上运行的交互式实例，在提交
MCQA 作业之前，需要创建/链接到一个 Session 上。
后续提交的 SQL 作业，均作为这一 Session 的子查询来执行。

目前 SDK 已将 Session 抽象为 SQLExecutor，因此通常用户无需关注 Session 类。

### SQLExecutor

`SQLExecutor` 是 MaxCompute 为执行 SQL 提供的统一的接口，用户可以通过这一接口，提交离线作业和 MCQA
作业，并提供了丰富的配置和方法，来满足用户多样的需求。

#### 使用方式

当创建`SQLExecutor`时，指定`ExecuteMode`为`INTERACTIVE`，则表示该`SQLExecutor`用于 MCQA 查询。
`SQLExecutor`的详细创建方法，和使用方法，参考文档：[SQLExecutor](../../api-reference/SQLExecutor.md)。

### 示例代码

```java
import ...;

public class SQLExecutorExample {

  public static void main(String args[]) {
    // 设置账号和项目信息。
    // 阿里云账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM用户进行API访问或日常运维，请登录RAM控制台创建RAM用户
    // 此处以把AccessKey 和 AccessKeySecret 保存在环境变量为例说明。您也可以根据业务需要，保存到配置文件里
    // 强烈建议不要把 AccessKey 和 AccessKeySecret 保存到代码里，会存在密钥泄漏风险
    Account
        account =
        new AliyunAccount(System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
                          System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"));
    Odps odps = new Odps(account);
    odps.setDefaultProject("<YOUR_PROJECT_NAME>");
    odps.setEndpoint("http://service.<regionid>.maxcompute.aliyun.com/api");

    // 准备构建SQLExecutor。
    SQLExecutorBuilder builder = SQLExecutorBuilder.builder();

    SQLExecutor sqlExecutor = null;
    try {
      // 创建一个使用 MCQA 的Executor。
      sqlExecutor = builder.odps(odps).executeMode(ExecuteMode.INTERACTIVE).build();

      // 如果需要的话可以传入查询的特殊设置。
      Map<String, String> queryHint = new HashMap<>();
      queryHint.put("odps.sql.mapper.split.size", "128");
      // 提交一个查询作业，支持传入Hint。
      sqlExecutor.run("select count(1) from test_table;", queryHint);

      // 列举一些支持的常用获取信息的接口。
      // UUID
      System.out.println("ExecutorId:" + sqlExecutor.getId());
      // 当前查询作业的logview。
      System.out.println("Logview:" + sqlExecutor.getLogView());
      // 当前查询作业的Instance对象(Interactive模式多个查询作业可能为同一个Instance)。
      System.out.println("InstanceId:" + sqlExecutor.getInstance().getId());
      // 当前查询作业的阶段进度(Console的进度条)。
      System.out.println("QueryStageProgress:" + sqlExecutor.getProgress());
      // 当前查询作业的执行状态变化日志，例如回退信息。
      System.out.println("QueryExecutionLog:" + sqlExecutor.getExecutionLog());

      ResultSet resultSet = sqlExecutor.getResultSet();
      while (resultSet.hasNext()) {
        System.out.println(resultSet.next());
      }

      // run another query
      sqlExecutor.run("select * from test_table;", new HashMap<>());

      // 获取查询结果的迭代器ResultSet，同步接口，可能会占用本线程直到查询成功或失败。
      // 获取大量结果数据时推荐使用，分次读取查询结果。
      ResultSet resultSet = sqlExecutor.getResultSet();
      while (resultSet.hasNext()) {
        System.out.println(resultSet.next());
      }

    } catch (OdpsException | IOException e) {
      e.printStackTrace();
    } finally {
      if (sqlExecutor != null) {
        // 关闭Executor释放相关资源。
        sqlExecutor.close();
      }
    }
  }
}
```

### 回退策略
对于 MCQA 加速失败的作业，默认会回退到离线作业进行查询。用户可以通过参数来调整回退策略。

