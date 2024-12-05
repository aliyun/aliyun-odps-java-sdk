---
title: 离线作业
sidebar_label: 离线作业
sidebar_position: 1
---

当前提交离线作业和获取作业结果，存在`SQLTask`和`SQLExecutor`两个类作为入口。

- `SQLTask`: 最原始的SQL任务入口。将执行SQL分为提交作业和查询结果两个步骤。
- `SQLExecutor`: 基于`SQLTask`的封装，提供了更便捷的接口，通过统一的接口，支持提交离线作业和MCQA作业。

## SQLTask

### 提交SQL作业

`SQLTask`类提供了多种方法来运行SQL查询。以下是这些方法的详细说明和示例代码。

#### 方法一：运行简单的SQL查询

使用`run`方法可以运行一个简单的SQL查询。

```java
public static Instance run(Odps odps,String sql)throws OdpsException
```

**参数**：

- `odps`：`Odps`对象。
- `sql`：需要执行的SQL查询。

**返回值**：

- 运行作业实例`Instance`。该作业运行项目使用`odps.getDefaultProject()`
  ，taskName使用默认值`AnonymousSQLTask`。

#### 方法二：运行指定运行项目，配置和别名SQL查询

使用`run`方法可以运行一个带有项目名称的SQL查询。

```java
public static Instance run(Odps odps,String project,String sql,Map<String, String> hints,Map<String, String> aliases)throws OdpsException
```

**参数**：

- `odps`：`Odps`对象。
- `project`：任务运行时所属的项目名称。
- `sql`：需要运行的SQL查询。
- `hints`：能够影响SQL执行的设置信息，例如：`odps.sql.type.system.odps2`等（可选）。
- `aliases`：Alias信息（可选）。

**返回值**：

- 运行作业实例`Instance`。该作业taskName使用默认值`AnonymousSQLTask`。

#### 方法三：运行指定任务名称的SQL查询

使用`run`方法可以运行一个带有任务名称的SQL查询。

```java
public static Instance run(Odps odps,String project,String sql,String taskName,Map<String, String> hints,Map<String, String> aliases)throws OdpsException
```

**参数**：

- `odps`：`Odps`对象。
- `project`：任务运行时所属的项目名称。
- `sql`：需要运行的SQL查询。
- `taskName`：任务名称（可选）。
- `hints`：能够影响SQL执行的设置信息（可选）。
- `aliases`：Alias信息（可选）。

**返回值**：

- 运行作业实例`Instance`。

#### 方法四：运行指定优先级的SQL查询

使用`run`方法可以运行一个带有优先级的SQL查询。

```java
public static Instance run(Odps odps,String project,String sql,String taskName,Map<String, String> hints,Map<String, String> aliases,Integer priority)throws OdpsException
```

**参数**：

- `odps`：`Odps`对象。
- `project`：任务运行时所属的项目名称。
- `sql`：需要运行的SQL查询。
- `taskName`：任务名称（可选）。
- `hints`：能够影响SQL执行的设置信息（可选）。
- `aliases`：Alias信息（可选）。
- `priority`：优先级（可选）。

**返回值**：

- 运行作业实例`Instance`。

### 获取作业结果

### 获取SQL查询结果

`SQLTask`类提供了多种方法来获取SQL查询结果。以下是这些方法的详细说明和示例代码。

#### 方法一：快速获取最多1万条记录的查询结果

使用`getResult`方法可以获取最多1万条记录的查询结果。每条记录类型均为`String`，忽略其在ODPS中的真实类型。

```java
public static List<Record> getResult(Instance instance)throws OdpsException;

public static List<Record> getResult(Instance instance,String taskName)throws OdpsException;
```

**参数**：

- `instance`：`Instance`对象。
- `taskName`：任务名称。默认 taskName 为 AnonymousSQLTask，如果用户作业提交时指定了其他
  taskName，则使用无该参数接口将无法获取到结果。

**返回值**：

- `ArrayRecord`列表。`Record`的中每条数据类型均为`String`，忽略各列在ODPS中的真实类型。

**示例代码**：

```java
String taskName="test_select_sql_task";
Instance instance=SQLTask.run(odps,odps.getDefaultProject(),"SELECT * FROM test_select_sql_result;",taskName,null,null,null);
instance.waitForSuccess();
List<Record> records=SQLTask.getResult(instance,taskName);
```

#### 方法二：使用 Instance Tunnel 获取查询结果 （最多1w条）

使用`getResultByInstanceTunnel`方法可以使用Instance
Tunnel获取查询结果。该方法返回的记录带有完整的Schema信息，出于内存限制，最多返回1w条记录。
如果用户需要获取更多记录，参考[方法三：使用 Instance Tunnel 分页获取查询结果](#方法三使用-instance-tunnel-分页获取查询结果)

```java
public static List<Record> getResultByInstanceTunnel(Instance instance)throws OdpsException,IOException;

public static List<Record> getResultByInstanceTunnel(Instance instance,String taskName)throws OdpsException,IOException;

public static List<Record> getResultByInstanceTunnel(Instance instance,Long limit)throws OdpsException,IOException;

public static List<Record> getResultByInstanceTunnel(Instance instance,String taskName,Long limit)throws OdpsException,IOException;
```

**参数**：

- `instance`：`Instance`对象。
- `taskName`：任务名称。默认值为`AnonymousSQLTask`。
- `limit`：获取结果的数量，默认值为`10000`。

**返回值**：

- 记录列表，最多返回1w条记录。

**示例代码**：

```java
String taskName="test_select_sql_task";
Instance instance=SQLTask.run(odps,odps.getDefaultProject(),"SELECT * FROM test_select_sql_result;",taskName,null,null,3);
instance.waitForSuccess();
List<Record> records=SQLTask.getResultByInstanceTunnel(instance,taskName,1000L);
```

#### 方法三：使用 Instance Tunnel 分页获取查询结果

使用`getResultSet`方法可以使用 instance tunnel 获取记录迭代器，从而可以让用户通过迭代器逐条获取记录来避免一次性获取全量数据到本地时撑爆内存的问题。
ResultSet 实现了 `Iterator<Record>`, `Iterable<Record>` 接口，可以直接使用迭代器进行遍历。

::: note
只有instance的owner本人可以使用本接口。
:::

```java

public static ResultSet getResultSet(Instance instance)throws OdpsException,IOException;

public static ResultSet getResultSet(Instance instance,String taskName)
    throws OdpsException,IOException;

public static ResultSet getResultSet(Instance instance,Long limit)throws OdpsException,IOException;

public static ResultSet getResultSet(Instance instance,String taskName,Long limit)
    throws OdpsException;

public static ResultSet getResultSet(Instance instance,String taskName,Long limit,boolean limitHint)
    throws OdpsException;

public static ResultSet getResultSet(Instance instance,String taskName,Long limit,
    boolean limitHint,URI tunnelEndpoint)throws OdpsException;

public static ResultSet getResultSet(Instance instance,String taskName,
    InstanceTunnel instanceTunnel,Long limit,boolean limitHint)
    throws OdpsException;
```

**参数**：

- `instance`：`Instance`对象。
- `taskName`：任务名称。默认值为`AnonymousSQLTask`。
- `limit`：获取结果的数量，默认值为`null`。
- `limitHint`
  ：是否限制结果数量（可选）。当limitHint为true时，结果最多只能获得1w条记录，超过将截断，但无需进行逐表的权限检查；当limitHint为false时，没有记录数限制，可获取instance对应query结果集的全量数据。但前提是需要逐表（SQL中
  涉及的表与视图）对用户进行权限检查，所以当查询涉及表所在project打开protection时，需要提前在policy中为相应表和视图添加exception，否则无权下载。
- `tunnelEndpoint`：指定的tunnel endpoint（可选），默认通过 endpoint 和 project 自动路由。
- `instanceTunnel`：允许用户使用自己创建的`InstanceTunnel`对象（可选）。

**返回值**：

- 记录迭代器 `ResultSet`。

**示例代码**：

```java
Instance instance = SQLTask.run(odps,"SELECT * FROM test_select_sql_result;");
instance.waitForSuccess();
ResultSet resultSet = SQLTask.getResultSet(instance);

while(resultSet.hasNext()){
    Record record = resultSet.next();
    System.out.println(record);
}
```

## SQLExecutor

### 使用方式

当创建`SQLExecutor`时，指定`ExecuteMode`为`OFFLINE`，则表示该`SQLExecutor`用于离线查询。

`SQLExecutor`的创建方法，和使用方法，参考文档：[SQLExecutor](../../api-reference/SQLExecutor.md)。

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
      // 创建一个默认执行离线SQL的Executor。
      sqlExecutor = builder.odps(odps).executeMode(ExecuteMode.OFFLINE).build();

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
