---
title: 控制台命令与 SDK 映射
description: MaxCompute 控制台常用命令（desc、setproject、set、show tables、read、kill 等）对应的 Java/Python/Go SDK 实现方式。
sidebar_position: 100
keywords:
  - desc
  - describe
  - setproject
  - set
  - show tables
  - show partitions
  - show instances
  - read
  - kill
  - wait
  - whoami
  - use project
  - list projects
  - add resource
  - create function
  - drop
  - grant
  - revoke
  - tunnel upload
  - tunnel download
  - cost
  - status
  - top instance
  - show flags
  - 控制台命令
  - odpscmd
---

# 控制台命令与 SDK 映射

本文列出 MaxCompute 控制台（odpscmd）最常用的命令，以及如何通过 SDK 实现相同功能。如果你习惯使用 odpscmd，可以快速找到 SDK 中对应的 API。

## 项目与会话

### use project — 切换默认项目

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
odps.setDefaultProject("my_project");
```

</TabItem>
<TabItem value="python" label="Python">

```python
odps.project = 'my_project'
```

</TabItem>
<TabItem value="go" label="Go">

```go
odpsIns.SetDefaultProjectName("my_project")
```

</TabItem>
</Tabs>

### describe project / desc project — 查看项目信息

对应 SDK 中 Project 对象的 reload 和属性读取方法。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Project project = odps.projects().get("my_project");
project.reload();
System.out.println("Owner: " + project.getOwner());
System.out.println("Comment: " + project.getComment());
System.out.println("创建时间: " + project.getCreatedTime());

// 获取项目属性
Map<String, String> properties = project.getProperties();
for (Map.Entry<String, String> entry : properties.entrySet()) {
    System.out.println(entry.getKey() + " = " + entry.getValue());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
project = odps.get_project('my_project')
print("Owner:", project.owner)
print("Comment:", project.comment)
print("创建时间:", project.creation_time)
```

</TabItem>
<TabItem value="go" label="Go">

```go
project := odpsIns.Projects().Get("my_project")
err := project.Load()
fmt.Println("Owner:", project.Owner())
fmt.Println("Comment:", project.Comment())
```

</TabItem>
</Tabs>

### setproject — 修改项目属性

setproject 命令可以查看或修改项目级别的属性，如 `odps.sql.type.system.odps2`、`odps.sql.decimal.odps2` 等。对应 SDK 中 Project 的 updateProject 方法。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 查看所有项目属性（等价于不带参数的 setproject）
Project project = odps.projects().get();
Map<String, String> properties = project.getAllProperties();
for (Map.Entry<String, String> e : properties.entrySet()) {
    System.out.println(e.getKey() + "=" + e.getValue());
}

// 修改项目属性（等价于 setproject key=value）
Map<String, String> newProps = new HashMap<>();
newProps.put("odps.sql.type.system.odps2", "true");
newProps.put("odps.sql.decimal.odps2", "true");
odps.projects().updateProject(project.getName(), newProps);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 查看项目属性
project = odps.get_project()
print(project.properties)

# 修改项目属性
project.update_project(properties={
    'odps.sql.type.system.odps2': 'true',
    'odps.sql.decimal.odps2': 'true',
})
```

</TabItem>
<TabItem value="go" label="Go">

```go
project := odpsIns.Projects().Get("my_project")
err := project.Load()

// 查看属性
props := project.Properties()
for k, v := range props {
    fmt.Printf("%s=%s\n", k, v)
}

// 修改属性
newProps := map[string]string{
    "odps.sql.type.system.odps2": "true",
}
err = project.UpdateProperties(newProps)
```

</TabItem>
</Tabs>

### set key=value — 设置会话级标志

set 命令设置的是会话级别的标志（hint），影响后续 SQL 执行行为，并不修改服务端配置。SDK 中通过在提交 SQL 任务时附加 hints 实现。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 在 SQL 执行时附加 hints
Map<String, String> hints = new HashMap<>();
hints.put("odps.sql.mapper.split.size", "256");
hints.put("odps.sql.reducer.instances", "10");

Instance instance = SQLTask.run(odps, odps.getDefaultProject(), sql, hints, null);
instance.waitForSuccess();
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 在 SQL 执行时附加 hints
hints = {
    'odps.sql.mapper.split.size': '256',
    'odps.sql.reducer.instances': '10',
}
odps.execute_sql(sql, hints=hints)
```

</TabItem>
<TabItem value="go" label="Go">

```go
hints := map[string]string{
    "odps.sql.mapper.split.size": "256",
    "odps.sql.reducer.instances": "10",
}
task := odps.NewSQLTask("query", sql, "", hints)
instance, err := odpsIns.ExecTask(task)
```

</TabItem>
</Tabs>

### list projects — 列出项目

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
ProjectFilter filter = new ProjectFilter();
filter.setOwner("ALIYUN$user@example.com");

Iterator<Project> it = odps.projects().iteratorByFilter(filter);
while (it.hasNext()) {
    Project p = it.next();
    System.out.println(p.getName());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
for project in odps.list_projects():
    print(project.name)
```

</TabItem>
<TabItem value="go" label="Go">

```go
projects := odpsIns.Projects()
err := projects.List(func(p *odps.Project, err error) {
    fmt.Println(p.Name())
})
```

</TabItem>
</Tabs>

### whoami — 查看当前用户

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
SecurityManager sm = odps.projects().get().getSecurityManager();
String result = sm.runQuery("whoami", false);
System.out.println(result);
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = odps.run_security_query("whoami")
print(result)
```

</TabItem>
</Tabs>

## 表操作

### describe table / desc table — 查看表结构

desc 命令是最常用的元数据查看命令。对应 SDK 中 Table 对象的 reload 和 getSchema 方法。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Table table = odps.tables().get("my_project", "my_table");
table.reload();

System.out.println("Owner: " + table.getOwner());
System.out.println("创建时间: " + table.getCreatedTime());
System.out.println("最后修改: " + table.getLastMetaModifiedTime());
System.out.println("生命周期: " + table.getLife());
System.out.println("注释: " + table.getComment());

// 列信息
for (Column col : table.getSchema().getColumns()) {
    System.out.printf("%-20s %-15s %s%n",
        col.getName(), col.getTypeInfo(), col.getComment());
}

// 分区列
for (Column col : table.getSchema().getPartitionColumns()) {
    System.out.printf("[分区列] %-20s %-15s%n", col.getName(), col.getTypeInfo());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
table = odps.get_table('my_table')
table.reload()
print("Owner:", table.owner)
print("Comment:", table.comment)
print("Schema:", table.table_schema)

for col in table.table_schema.columns:
    print(f"{col.name:20s} {col.type:15s} {col.comment or ''}")
```

</TabItem>
<TabItem value="go" label="Go">

```go
table := odpsIns.Tables().Get("my_table")
err := table.Load()

fmt.Println("Owner:", table.Owner())
fmt.Println("Comment:", table.Comment())

schema := table.Schema()
for _, col := range schema.Columns {
    fmt.Printf("%-20s %-15s %s\n", col.Name, col.Type, col.Comment)
}
```

</TabItem>
</Tabs>

### describe table partition / desc table partition — 查看分区详情

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// desc table my_table partition(dt='20250101')
Table table = odps.tables().get("my_table");
Partition partition = table.getPartition(new PartitionSpec("dt='20250101'"));
partition.reload();

System.out.println("创建时间: " + partition.getCreatedTime());
System.out.println("大小: " + partition.getSize());
System.out.println("记录数: " + partition.getRecordNum());
```

</TabItem>
<TabItem value="python" label="Python">

```python
table = odps.get_table('my_table')
partition = table.get_partition("dt='20250101'")
print("大小:", partition.size)
print("记录数:", partition.record_num)
```

</TabItem>
<TabItem value="go" label="Go">

```go
table := odpsIns.Tables().Get("my_table")
partition := table.GetPartition(odps.NewPartitionSpec("dt='20250101'"))
err := partition.Load()
fmt.Println("大小:", partition.Size())
```

</TabItem>
</Tabs>

### show tables — 列出表

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// show tables
Iterator<Table> it = odps.tables().iterator("my_project");
while (it.hasNext()) {
    Table t = it.next();
    System.out.println(t.getName());
}

// show tables like 'user%'（按前缀过滤）
TableFilter filter = new TableFilter();
filter.setName("user");
Iterator<Table> filtered = odps.tables().iterator("my_project", filter);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# show tables
for table in odps.list_tables():
    print(table.name)

# show tables like 'user%'
for table in odps.list_tables(prefix='user'):
    print(table.name)
```

</TabItem>
<TabItem value="go" label="Go">

```go
tables := odpsIns.Tables()
err := tables.List(func(t *odps.Table, err error) {
    fmt.Println(t.Name())
})
```

</TabItem>
</Tabs>

### show partitions — 列出分区

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Table table = odps.tables().get("my_table");
Iterator<Partition> it = table.getPartitionIterator();
while (it.hasNext()) {
    Partition p = it.next();
    System.out.println(p.getPartitionSpec());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
table = odps.get_table('my_table')
for partition in table.partitions:
    print(partition.name)
```

</TabItem>
<TabItem value="go" label="Go">

```go
table := odpsIns.Tables().Get("my_table")
partitions, err := table.GetPartitions()
for _, p := range partitions {
    fmt.Println(p.PartitionSpec())
}
```

</TabItem>
</Tabs>

### read table — 预览表数据

read 命令用于快速预览表中的少量数据，最多返回 1 万行。对应 SDK 中 Table 的 read 方法。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// read my_table 100
Table table = odps.tables().get("my_table");
RecordReader reader = table.read(100);
Record record;
while ((record = reader.read()) != null) {
    System.out.println(record.get(0));
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 等价于 read my_table 100
table = odps.get_table('my_table')
with table.open_reader() as reader:
    for record in reader[:100]:
        print(record[0])
```

</TabItem>
</Tabs>

详见 [数据预览](./read-data/preview.md)。

### drop table — 删除表

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// drop table my_table
odps.tables().delete("my_table");

// drop table if exists my_table
odps.tables().delete("my_table", true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
odps.delete_table('my_table', if_exists=True)
```

</TabItem>
<TabItem value="go" label="Go">

```go
err := odpsIns.Tables().Delete("my_table", true)
```

</TabItem>
</Tabs>

## 实例操作

### show instances / show p — 列出实例

show instances（简写 show p）列出当前项目下的实例。对应 SDK 中的实例迭代器。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
InstanceFilter filter = new InstanceFilter();
filter.setStatus(Instance.Status.RUNNING);

Iterator<Instance> it = odps.instances().iterator(filter);
while (it.hasNext()) {
    Instance inst = it.next();
    System.out.printf("%s  %s  %s%n", inst.getId(), inst.getStatus(), inst.getStartTime());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
for instance in odps.list_instances(status='Running'):
    print(instance.id, instance.status, instance.start_time)
```

</TabItem>
<TabItem value="go" label="Go">

```go
instances := odpsIns.Instances()
err := instances.List(func(inst *odps.Instance, err error) {
    fmt.Printf("%s  %s\n", inst.Id(), inst.Status())
})
```

</TabItem>
</Tabs>

### status instance — 查看实例状态

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Instance instance = odps.instances().get("instance_id");
System.out.println("状态: " + instance.getStatus());

// 查看各 Task 的状态
Map<String, Instance.TaskStatus> taskStatus = instance.getTaskStatus();
for (Map.Entry<String, Instance.TaskStatus> entry : taskStatus.entrySet()) {
    System.out.printf("Task %s: %s%n", entry.getKey(), entry.getValue().getStatus());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
instance = odps.get_instance('instance_id')
print("状态:", instance.status)
print("任务状态:", instance.get_task_statuses())
```

</TabItem>
<TabItem value="go" label="Go">

```go
instance := odpsIns.Instances().Get("instance_id")
err := instance.Load()
fmt.Println("状态:", instance.Status())
```

</TabItem>
</Tabs>

### kill instance — 终止实例

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Instance instance = odps.instances().get("instance_id");
instance.stop();
```

</TabItem>
<TabItem value="python" label="Python">

```python
instance = odps.get_instance('instance_id')
instance.stop()
```

</TabItem>
<TabItem value="go" label="Go">

```go
instance := odpsIns.Instances().Get("instance_id")
err := instance.Terminate()
```

</TabItem>
</Tabs>

### wait instance — 等待实例完成

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Instance instance = odps.instances().get("instance_id");
instance.waitForSuccess();

// 获取结果
Map<String, String> results = instance.getTaskResults();
System.out.println(results.get("AnonymousSQLTask"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
instance = odps.get_instance('instance_id')
instance.wait_for_success()
print(instance.get_task_results())
```

</TabItem>
<TabItem value="go" label="Go">

```go
instance := odpsIns.Instances().Get("instance_id")
err := instance.WaitForSuccess()
```

</TabItem>
</Tabs>

## 资源与函数

### add resource / add file / add jar / add py — 上传资源

add 命令用于上传文件、JAR 包或 Python 脚本作为 MaxCompute 资源。对应 SDK 中的 Resources.create 方法。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// add file my_file.txt
FileResource resource = new FileResource();
resource.setName("my_file.txt");
try (FileInputStream in = new FileInputStream("/path/to/my_file.txt")) {
    odps.resources().create(resource, in);
}

// add jar my_udf.jar -f（-f 表示覆盖更新）
JarResource jarRes = new JarResource();
jarRes.setName("my_udf.jar");
try (FileInputStream in = new FileInputStream("/path/to/my_udf.jar")) {
    odps.resources().update(jarRes, in);
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# add file my_file.txt
odps.create_resource('my_file.txt', 'file', fileobj=open('/path/to/my_file.txt', 'rb'))

# add jar my_udf.jar -f
odps.create_resource('my_udf.jar', 'jar', fileobj=open('/path/to/my_udf.jar', 'rb'))
```

</TabItem>
<TabItem value="go" label="Go">

```go
resource := odps.NewFileResource("my_file.txt")
err := odpsIns.Resources().Create(resource, "/path/to/my_file.txt")
```

</TabItem>
</Tabs>

详见 [资源管理](./manage-resources/resources.md)。

### create function — 创建函数

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// create function my_udf as 'com.example.MyUDF' using 'my_udf.jar'
Function function = new Function();
function.setName("my_udf");
function.setClassPath("com.example.MyUDF");
function.setResources(Collections.singletonList(odps.resources().get("my_udf.jar")));
odps.functions().create(function);
```

</TabItem>
<TabItem value="python" label="Python">

```python
odps.create_function('my_udf',
                     class_type='com.example.MyUDF',
                     resources=['my_udf.jar'])
```

</TabItem>
<TabItem value="go" label="Go">

```go
function := odps.NewFunction("my_udf", "com.example.MyUDF", []string{"my_udf.jar"})
err := odpsIns.Functions().Create(function)
```

</TabItem>
</Tabs>

详见 [函数管理](./manage-resources/functions.md)。

### show resources / show functions — 列出资源和函数

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// show resources
Iterator<Resource> resIt = odps.resources().iterator();
while (resIt.hasNext()) {
    Resource r = resIt.next();
    System.out.printf("%s  %s  %s%n", r.getName(), r.getType(), r.getLastModifiedTime());
}

// show functions
Iterator<Function> funcIt = odps.functions().iterator();
while (funcIt.hasNext()) {
    Function f = funcIt.next();
    System.out.printf("%s  %s%n", f.getName(), f.getClassPath());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# show resources
for resource in odps.list_resources():
    print(resource.name, resource.type)

# show functions
for func in odps.list_functions():
    print(func.name, func.class_type)
```

</TabItem>
<TabItem value="go" label="Go">

```go
// show resources
odpsIns.Resources().List(func(r *odps.Resource, err error) {
    fmt.Println(r.Name(), r.Type())
})

// show functions
odpsIns.Functions().List(func(f *odps.Function, err error) {
    fmt.Println(f.Name(), f.ClassPath())
})
```

</TabItem>
</Tabs>

### drop resource / drop function — 删除资源和函数

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// drop resource my_file.txt
odps.resources().delete("my_file.txt");

// drop function my_udf
odps.functions().delete("my_udf");
```

</TabItem>
<TabItem value="python" label="Python">

```python
odps.delete_resource('my_file.txt')
odps.delete_function('my_udf')
```

</TabItem>
<TabItem value="go" label="Go">

```go
odpsIns.Resources().Delete("my_file.txt")
odpsIns.Functions().Delete("my_udf")
```

</TabItem>
</Tabs>

## 权限与安全

### grant / revoke — 授权与撤销

grant 和 revoke 命令用于管理 ACL 权限。对应 SDK 中 SecurityManager 的 runQuery 方法。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
SecurityManager sm = odps.projects().get().getSecurityManager();

// grant Select on table my_table to user ALIYUN$user@example.com
sm.runQuery("GRANT Select ON TABLE my_table TO USER ALIYUN$user@example.com", false);

// revoke Select on table my_table from user ALIYUN$user@example.com
sm.runQuery("REVOKE Select ON TABLE my_table FROM USER ALIYUN$user@example.com", false);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# grant
odps.run_security_query("GRANT Select ON TABLE my_table TO USER ALIYUN$user@example.com")

# revoke
odps.run_security_query("REVOKE Select ON TABLE my_table FROM USER ALIYUN$user@example.com")
```

</TabItem>
</Tabs>

### show grants — 查看权限

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
SecurityManager sm = odps.projects().get().getSecurityManager();

// show grants for current user
String result = sm.runQuery("SHOW GRANTS", false);
System.out.println(result);

// show grants for specific user
result = sm.runQuery("SHOW GRANTS FOR USER ALIYUN$user@example.com", false);
System.out.println(result);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# show grants
result = odps.run_security_query("SHOW GRANTS")
print(result)
```

</TabItem>
</Tabs>

详见 [权限校验](./security/check-permission.md) 和 [ACL 查询](./security/acl-query.md)。

### show securityconfiguration — 查看安全配置

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
SecurityManager sm = odps.projects().get().getSecurityManager();
SecurityConfiguration config = sm.getSecurityConfiguration();

System.out.println("LabelSecurity: " + config.isLabelSecurityEnabled());
System.out.println("ProjectProtection: " + config.isProjectProtectionEnabled());
System.out.println("ObjectCreatorHasAccess: " + config.isObjectCreatorHasAccessEnabled());
System.out.println("ObjectCreatorHasGrant: " + config.isObjectCreatorHasGrantEnabled());
```

</TabItem>
<TabItem value="python" label="Python">

```python
result = odps.run_security_query("SHOW SECURITYCONFIGURATION")
print(result)
```

</TabItem>
</Tabs>

## SQL 执行

### SQL 查询 — 执行 SQL 语句

控制台中直接输入 SQL 语句即可执行。SDK 中通过 SQLTask 提交。

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Instance instance = SQLTask.run(odps, "SELECT * FROM my_table LIMIT 10;");
instance.waitForSuccess();
Map<String, String> results = instance.getTaskResults();
System.out.println(results.get("AnonymousSQLTask"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
with odps.execute_sql('SELECT * FROM my_table LIMIT 10').open_reader() as reader:
    for record in reader:
        print(record)
```

</TabItem>
<TabItem value="go" label="Go">

```go
instance, err := odpsIns.ExecSql("SELECT * FROM my_table LIMIT 10")
err = instance.WaitForSuccess()
results, err := instance.GetResult()
fmt.Println(results)
```

</TabItem>
</Tabs>

详见 [执行 SQL](./execute-sql/offline.md)。

### cost sql — 预估 SQL 费用

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
// 通过 dry run 模式估算费用
Instance instance = SQLTask.run(odps, odps.getDefaultProject(),
    "SELECT * FROM my_table;", "SQL", null, null, null, true);
instance.waitForSuccess();
System.out.println(instance.getTaskResults().get("AnonymousSQLTask"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
# 预估 SQL 费用
cost = odps.execute_sql_cost('SELECT * FROM my_table')
print("Input bytes:", cost.input_size)
```

</TabItem>
</Tabs>

## 数据通道

### tunnel upload — 上传数据

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
TableTunnel tunnel = new TableTunnel(odps);
UploadSession session = tunnel.buildUploadSession()
    .setProjectName("my_project")
    .setTableName("my_table")
    .build();

try (TunnelRecordWriter writer = session.openRecordWriter(0)) {
    Record record = session.newRecord();
    record.set(0, "value");
    writer.write(record);
}
session.commit(new long[]{0});
```

</TabItem>
<TabItem value="python" label="Python">

```python
with odps.get_table('my_table').open_writer() as writer:
    writer.write([['value1'], ['value2']])
```

</TabItem>
<TabItem value="go" label="Go">

```go
tunnel := odps.NewTunnel(odpsIns)
session, err := tunnel.CreateUploadSession("my_project", "my_table")
writer, err := session.OpenRecordWriter(0)
// write records...
writer.Close()
session.Commit([]int{0})
```

</TabItem>
</Tabs>

详见 [Tunnel 上传](./write-data/tunnel-upload.md)。

### tunnel download — 下载数据

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
TableTunnel tunnel = new TableTunnel(odps);
DownloadSession session = tunnel.buildDownloadSession()
    .setProjectName("my_project")
    .setTableName("my_table")
    .build();

try (TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount())) {
    while (reader.hasNext()) {
        Record record = reader.next();
        System.out.println(record.get(0));
    }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
with odps.get_table('my_table').open_reader() as reader:
    for record in reader:
        print(record[0])
```

</TabItem>
<TabItem value="go" label="Go">

```go
tunnel := odps.NewTunnel(odpsIns)
session, err := tunnel.CreateDownloadSession("my_project", "my_table")
reader, err := session.OpenRecordReader(0, session.RecordCount(), nil)
// read records...
```

</TabItem>
</Tabs>

详见 [Tunnel 下载](./read-data/tunnel-download.md)。

## Quota 管理

### show quotas — 列出配额

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Iterator<Quota> it = odps.quotas().iterator();
while (it.hasNext()) {
    Quota q = it.next();
    System.out.println(q.getName());
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
for quota in odps.list_quotas():
    print(quota.name)
```

</TabItem>
</Tabs>

### desc quota — 查看配额详情

<Tabs groupId="sdk-language">
<TabItem value="java" label="Java" default>

```java
Quota quota = odps.quotas().get("quota_name");
System.out.println("名称: " + quota.getName());
System.out.println("集群: " + quota.getCluster());
```

</TabItem>
<TabItem value="python" label="Python">

```python
quota = odps.get_quota('quota_name')
print("名称:", quota.name)
```

</TabItem>
</Tabs>

## 快速对照表

| 控制台命令 | SDK 对应 | 文档链接 |
|-----------|---------|---------|
| `use project` | `odps.setDefaultProject()` | — |
| `desc project` | `Project.reload()` + 属性方法 | — |
| `setproject key=val` | `Projects.updateProject()` | — |
| `set key=val` | SQL hints 参数 | — |
| `list projects` | `Projects.iteratorByFilter()` | — |
| `whoami` | `SecurityManager.runQuery("whoami")` | — |
| `desc table` | `Table.reload()` + `Table.getSchema()` | [修改表](./manage-tables/alter-table.md) |
| `show tables` | `Tables.iterator()` | [创建表](./manage-tables/create-table.md) |
| `show partitions` | `Table.getPartitionIterator()` | [分区管理](./manage-tables/partitions.md) |
| `read table` | `Table.read()` | [数据预览](./read-data/preview.md) |
| `drop table` | `Tables.delete()` | — |
| `show instances` / `show p` | `Instances.iterator()` | — |
| `status instance` | `Instance.getStatus()` | — |
| `kill instance` | `Instance.stop()` | — |
| `wait instance` | `Instance.waitForSuccess()` | — |
| `add file/jar/py` | `Resources.create()` | [资源管理](./manage-resources/resources.md) |
| `create function` | `Functions.create()` | [函数管理](./manage-resources/functions.md) |
| `show resources` | `Resources.iterator()` | [资源管理](./manage-resources/resources.md) |
| `show functions` | `Functions.iterator()` | [函数管理](./manage-resources/functions.md) |
| `drop resource` | `Resources.delete()` | [资源管理](./manage-resources/resources.md) |
| `drop function` | `Functions.delete()` | [函数管理](./manage-resources/functions.md) |
| `grant/revoke` | `SecurityManager.runQuery()` | [ACL 查询](./security/acl-query.md) |
| `show grants` | `SecurityManager.runQuery()` | [权限校验](./security/check-permission.md) |
| SQL 语句 | `SQLTask.run()` | [执行 SQL](./execute-sql/offline.md) |
| `cost sql` | SQLTask dry run | — |
| `tunnel upload` | `TableTunnel` + `UploadSession` | [Tunnel 上传](./write-data/tunnel-upload.md) |
| `tunnel download` | `TableTunnel` + `DownloadSession` | [Tunnel 下载](./read-data/tunnel-download.md) |
| `show quotas` | `Quotas.iterator()` | — |
| `desc quota` | `Quotas.get()` | — |
