---
title: Project
sidebar_position: 2
---

# 项目管理

## 概览
项目管理包含两个核心类：`Projects`（项目集合管理器）和`Project`（项目实体）。
`Projects`类提供项目级的增删改查操作，`Project`类封装单个项目的属性和行为。二者共同构成项目管理的正交体系，支持托管项目和外部数据源项目的统一管理。

## Projects 类
作为项目集合的入口控制器，负责：
1. 项目生命周期管理（创建/删除/更新）
2. 项目查询与过滤
3. 跨项目操作协调
4. 资源分配调度

### 方法

#### 获取项目
```java
public Project get() throws OdpsException
public Project get(String projectName) throws OdpsException
```

- **提示**  
  `Project` 对象实现了 `LazyLoad` 接口，在首次调用时加载项目属性，调用 get 方法不会加载完整项目属性。
  高频调用建议配合缓存机制，根据`endpoint`和`projectName`缓存`Project`对象，并按需通过`Project.reload`方法手动重新加载。

#### 判断项目是否存在
```java
public boolean exists(String projectName) throws OdpsException
```

##### 遍历项目

#### 快捷遍历方法
```java
// 按Owner遍历
public Iterator<Project> iterator(String owner) 

// 获取Iterable接口（支持foreach语法）
public Iterable<Project> iterable(String owner)
```

##### 过滤查询接口
```java
public Iterator<Project> iteratorByFilter(ProjectFilter filter)
```
通过`ProjectFilter`实现多条件过滤查询

**过滤条件参数**
```java
public class ProjectFilter {
  // 基础过滤条件
  private String owner;     // 项目所有者（精确匹配）
  private String name;      // 项目名称前缀匹配
  private String user;      // 项目使用者
  private String groupName; // 项目组名称
  private String tenantId;  // 租户ID
  private String regionId;  // 地域ID

  // 高级过滤条件
  private String quotaNickname; // 配额别名
  private String quotaType;     // 配额类型
  private String quotaName;     // 配额名称
  private Boolean enableDr;     // 是否开启存储容灾
}
```



#### 创建项目
```java
public void create(CreateProjectParam param) throws OdpsException
```

**参数构建器方法**

| 方法                               | 说明           | 是否必填 |
|----------------------------------|--------------|------|
| `name(String)`                   | 项目名称（2-64字符） | ✅    |
| `owner(String)`                  | 项目所有者        | ✅    |
| `defaultCluster(String)`         | 默认计算集群       | ✅    |
| `comment(String)`                | 项目描述         | ❌    |
| `superAdmin(String)`             | 超级管理员        | ❌    |
| `properties(Map)`                | 项目属性（覆盖设置）   | ❌    |
| `appendProperty(String, String)` | 追加属性         | ❌    |
| `groupName(String)`              | 项目组名称        | ❌    |
| `defaultQuotaId(String)`         | 默认计算配额ID     | ❌    |
| `defaultCtrlService(String)`     | 默认控制服务地址     | ❌    |


#### 创建外部项目
```java
public void createExternalProject(String projectName, String comment, 
                                 String refProjectName, 
                                 Project.ExternalProjectProperties extProperties)
```
创建MC外部项目，实现异构数据源联邦查询，将Hive等外部系统映射到MC中。

- **使用示例**
  ```java
  Project.ExternalProjectProperties extProps = new Project.ExternalProjectProperties("hive");  
  // required properties for 'hive' source  
  extProps.addProperty("hms.ips", "10.0.0.1:5300,10.0.0.2:5300");  
  extProps.addProperty("hive.database.name", "odps");  
  extProps.addProperty("hdfs.namenode.ips", "192.168.0.12:3829,192.168.0.5:3389");  
  // network properties  
  extProps.addNetworkProperty("odps.external.net.vpc", "false")
  
  odps.projects().createExternalProject(projectName, comment, refProjectName, extProps);
  ```

#### 删除操作
```java
public Instance delete(String projectName, boolean isImmediate)
```
- **删除模式**  
  | 模式 | 资源回收 | 可恢复性 | 返回类型 |
  |------|---------|----------|---------|
  | 逻辑删除 | 延迟执行 | 支持回收站恢复 | null    |
  | 物理删除 | 立即释放 | 不可逆   | 操作实例 |



---

## Project 类

Project 类负责对具体某个项目进行属性访问和操作

### 基础属性访问

#### 获取项目类型
```java
public ProjectType getType();
```
**说明**：返回项目的存储类型，用于区分托管项目与外部项目  
**枚举定义**：
```java
public enum ProjectType {
  MANAGED,      // 原生存储项目（默认类型）
  EXTERNAL,     // 旧版外部存储（兼容模式）
  EXTERNAL_V2   // 新版外部存储（推荐）
}
```

#### 获取项目描述
```java
public String getComment();
```
**说明**：返回项目创建时设置的描述信息，未设置时返回空字符串

#### 获取项目所有者
```java
public String getOwner();
```
**说明**：返回项目创建者账号（主账号或RAM子账号）

#### 获取所属地域
```java
public String getRegionId();
```
**说明**：返回项目部署的阿里云地域ID（如`cn-shanghai`）

#### 获取创建时间
```java
public Date getCreatedTime();
```
**说明**：返回项目创建时间的`Date`对象

#### 获取最后修改时间
```java
public Date getLastModifiedTime();
```
**说明**：返回项目属性最后一次修改的时间

#### 获取项目状态
```java
public Status getStatus();
```
**说明**：返回项目当前运行状态  
**状态枚举**：
```java
public enum Status {
  AVAILABLE,   // 正常可用
  READONLY,    // 只读模式（通常由欠费触发）
  DELETING,    // 删除流程中
  FROZEN,      // 被系统冻结
  UNKOWN       // 状态未知（需调用reload()刷新）
}
```


### 配置管理

#### 获取显式配置属性
```java
public Map<String, String> getProperties();
```
**说明**：返回项目创建/更新时显式设置的属性集合（不包含继承配置）

#### 获取全部配置属性
```java
public Map<String, String> getAllProperties();
```
**说明**：包含显式配置+从项目组（Group）继承的属性

#### 获取指定配置
```java
public String getProperty(String key);
```
**参数**：`key` - 配置项名称（如`odps.sql.type.system.odps2`）

#### 获取扩展属性
```java
public Map<String, String> getExtendedProperties();
```
**说明**：返回项目扩展配置信息（非核心业务属性）


### 存储信息

#### 获取分层存储详情
```java
public StorageTierInfo getStorageTierInfo();
```
**说明**：需先调用`reload()`刷新数据，否则可能返回过期信息  
**返回结构**：
```java
public class StorageTierInfo {
  public String storageTier;          // 当前存储策略（STANDARD/LOWFREQUENCY/LONGTERM）
  public Date storageLastModifiedTime;// 最后统计时间
  public Map<String, Long> storageSize; // 各层级存储量（单位：Byte）
}
```

### 安全相关操作
如授权操作，查询权限等，详情查看[权限控制](../core-concept/permission/index.mdx)
```java
public SecurityManager getSecurityManager()
```

### 网络接入

#### 获取Tunnel接入点
```java
public String getTunnelEndpoint(String quotaName);
```
**参数说明**：
- `quotaName`：计算配额名称，传`null`时使用项目默认配额