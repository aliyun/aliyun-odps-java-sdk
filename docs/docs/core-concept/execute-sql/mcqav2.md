---
title: MaxQA
sidebar_label: MaxQA（MCQA 2.0）
sidebar_position: 3
---

:::note
了解 MaxQA
的特性、架构和使用方式，请参考[阿里云官网文档](https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-query-acceleration2-0)
:::

odps-sdk 从 0.52.0 版本开始，支持提交 MaxQA 作业，低于 0.52.0 版本，可能存在兼容性问题，详见 CHANGELOG

## 核心接口

### SQLExecutor

`SQLExecutor` 是 MaxCompute 为执行 SQL 提供的统一的接口，用户可以通过这一接口，提交离线作业和 MCQA
作业，并提供了丰富的配置和方法，来满足用户多样的需求。

#### 使用方式

当创建`SQLExecutor`时，指定`ExecuteMode`为`INTERACTIVE`
，指定MCQA2.0交互式资源名（quotaName），并开启MCQA2.0（enableMcqaV2)，则表示该`SQLExecutor`用于 MCQA 2.0
查询。
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
      // 创建一个默认使用 MCQA2.0 模式的Executor。
      sqlExecutor = builder.odps(odps)
          .executeMode(ExecuteMode.INTERACTIVE)
          .quotaName("<YOUR_INTERACTIVE_QUOTA_NICKNAME>")
          .enableMcqaV2(true)
          .build();

      // 如果需要的话可以传入查询的特殊设置。
      Map<String, String> queryHint = new HashMap<>();
      queryHint.put("odps.sql.mapper.split.size", "128");
      // 提交一个查询作业，支持传入Hint。
      sqlExecutor.run("select count(1) from test_table;", queryHint);
      
      // 当前查询作业的logview。
      System.out.println("Logview:" + sqlExecutor.getLogView());
      // 当前查询作业的InstanceId
      System.out.println("InstanceId:" + sqlExecutor.getQueryId());
      
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