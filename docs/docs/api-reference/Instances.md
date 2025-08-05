---
title: Instances
sidebar_position: 4
---

# Instances

`Instances` 类用于管理 MaxCompute 项目中的实例。实例是 MaxCompute 中任务执行的基本单位，每个 SQL 查询、MapReduce 任务等都会创建一个实例。

## 概览

- [获取 Instances 实例](#获取-instances-实例)
- [创建实例](#创建实例)
- [获取实例](#获取实例)
- [检查实例是否存在](#检查实例是否存在)
- [遍历实例](#遍历实例)
- [遍历队列中的实例](#遍历队列中的实例)

## 获取 Instances 实例

要操作实例，首先需要获取 Instances 实例：

```java
Instances instances = odps.instances();
```

## 创建实例

### 创建任务实例

```java
// 创建单个任务实例
Task task = new SQLTask();
task.setQuery("SELECT * FROM table_name");
Instance instance = instances.create(task);

// 创建带优先级的任务实例
Instance instance = instances.create(task, 10); // 优先级为10

// 创建带优先级和任务名称的任务实例
Instance instance = instances.create(task, 10, "my_task_name");

// 在指定项目中创建任务实例
Instance instance = instances.create("project_name", task);

// 在指定项目中创建带优先级的任务实例
Instance instance = instances.create("project_name", task, 10);

// 在指定项目中创建带优先级和任务名称的任务实例
Instance instance = instances.create("project_name", task, 10, "my_task_name");
```

### 创建作业实例

```java
// 创建作业实例
Job job = new Job();
// ... 配置作业 ...
Instance instance = instances.create(job);

// 在指定项目中创建作业实例
Instance instance = instances.create("project_name", job);
```

### 使用选项创建实例

```java
// 使用 CreateInstanceOption 创建实例
CreateInstanceOption option = new CreateInstanceOption();
option.setPriority(10);
option.setTaskName("my_task");
Instance instance = instances.create(task, option);
```

## 获取实例

```java
// 获取默认项目中的实例
Instance instance = instances.get("instance_id");

// 获取指定项目中的实例
Instance instance = instances.get("project_name", "instance_id");
```

## 检查实例是否存在

```java
// 检查默认项目中实例是否存在
boolean exists = instances.exists("instance_id");

// 检查指定项目中实例是否存在
boolean exists = instances.exists("project_name", "instance_id");
```

## 遍历实例

### 使用迭代器遍历

```java
// 遍历默认项目中的所有实例
Iterator<Instance> iterator = instances.iterator();

// 遍历指定项目中的所有实例
Iterator<Instance> iterator = instances.iterator("project_name");

// 使用过滤器遍历实例
InstanceFilter filter = new InstanceFilter();
filter.setStatus(Instance.Status.RUNNING);
Iterator<Instance> iterator = instances.iterator("project_name", filter);
```

### 使用 Iterable 遍历

```java
// 获取默认项目中实例的 Iterable
Iterable<Instance> iterable = instances.iterable();

// 获取指定项目中实例的 Iterable
Iterable<Instance> iterable = instances.iterable("project_name");

// 使用过滤器获取实例的 Iterable
InstanceFilter filter = new InstanceFilter();
filter.setOwner("user_id");
Iterable<Instance> iterable = instances.iterable("project_name", filter);

// 使用 foreach 语法遍历
for (Instance instance : instances.iterable()) {
    System.out.println(instance.getId());
}
```

### InstanceFilter 使用

```java
// 创建过滤器
InstanceFilter filter = new InstanceFilter();
filter.setOwner("user_id"); // 按所有者过滤
filter.setStatus(Instance.Status.RUNNING); // 按状态过滤
filter.setTaskName("my_task"); // 按任务名称过滤

// 使用过滤器遍历
Iterator<Instance> iterator = instances.iterator("project_name", filter);
```

## 遍历队列中的实例

```java
// 遍历队列中的实例
Iterator<Instance.InstanceQueueingInfo> queueIterator = instances.iteratorQueueing();

// 在指定项目中遍历队列中的实例
Iterator<Instance.InstanceQueueingInfo> queueIterator = instances.iteratorQueueing("project_name");

// 使用过滤器遍历队列中的实例
InstanceFilter filter = new InstanceFilter();
filter.setOwner("user_id");
Iterator<Instance.InstanceQueueingInfo> queueIterator = instances.iteratorQueueing("project_name", filter);
```