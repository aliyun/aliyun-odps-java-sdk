---
title: UDF 函数开发与使用
sidebar_position: 6
---

# UDF 函数开发与使用

在 MaxCompute 中，用户自定义函数 (UDF) 是扩展 SQL 功能的重要方式。通过合理开发和使用 UDF，可以提高数据处理的效率和灵活性。

## 目录
[UDF 开发完整流程](#udf-开发完整流程)
[创建 Maven 项目](#创建-maven-项目)
[UDF 实现示例](#udf-实现示例)
[ExecutionContext 详解](#executioncontext-详解)
[打包和部署](#打包和部署)
[函数注册和使用](#函数注册和使用)

## UDF 开发完整流程

开发和使用 UDF 需要经过以下几个步骤：

1. 创建 Maven 项目
2. 实现 UDF 类
3. 打包项目为 JAR 文件
4. 上传 JAR 文件作为资源
5. 注册函数
6. 在 SQL 中使用函数

## 创建 Maven 项目

首先创建一个 Maven 项目来开发 UDF。以下是一个典型的 pom.xml 文件配置：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    
    <groupId>com.example</groupId>
    <artifactId>odps-udf-example</artifactId>
    <version>1.0-SNAPSHOT</version>
    
    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    
    <dependencies>
        <!-- MaxCompute UDF SDK -->
        <dependency>
            <groupId>com.aliyun.odps</groupId>
            <artifactId>odps-sdk-udf</artifactId>
            <version>0.53.0-public</version>
            <scope>provided</scope>
        </dependency>
        
        <!-- 其他依赖 -->
        <dependency>
            <groupId>commons-lang</groupId>
            <artifactId>commons-lang</artifactId>
            <version>2.6</version>
        </dependency>
    </dependencies>
    
    <build>
        <plugins>
            <!-- 使用 shade plugin 打包所有依赖 -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.2.4</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

关键配置说明：
- `odps-sdk-udf` 依赖的 scope 设置为 `provided`，因为运行时环境中已经提供了这个依赖
- 使用 `maven-shade-plugin` 来打包所有依赖到一个 JAR 文件中

## UDF 实现示例

以下是一个简单的 UDF 示例，用于计算字符串中某个字符出现的次数：

```java
package com.example.udf;

import com.aliyun.odps.udf.UDF;

/**
 * 计算字符串中指定字符出现的次数
 */
public class CharacterCount extends UDF {
    
    /**
     * 计算字符串中指定字符出现的次数
     * 
     * @param input 输入字符串
     * @param character 要计算的字符
     * @return 字符出现的次数
     */
    public Integer evaluate(String input, String character) {
        // 参数验证
        if (input == null || character == null || character.length() != 1) {
            return 0;
        }
        
        char targetChar = character.charAt(0);
        int count = 0;
        
        // 计算字符出现次数
        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == targetChar) {
                count++;
            }
        }
        
        return count;
    }
}
```

更复杂的 UDF 示例，带 ExecutionContext 使用：

```java
package com.example.udf;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;

/**
 * 带计数器的 UDF 示例
 */
public class WordCountWithCounter extends UDF {
    
    private ExecutionContext ctx;
    
    @Override
    public void setup(ExecutionContext ctx) {
        this.ctx = ctx;
    }
    
    public String evaluate(String sentence) throws UDFException {
        if (sentence == null) {
            return null;
        }
        
        try {
            // 获取计数器
            // 通过枚举方式获取计数器
            Counter processCounter = ctx.getCounter(MyCounter.PROCESS_COUNT);
            
            // 通过组名和计数器名获取计数器
            Counter wordCounter = ctx.getCounter("WORD_PROCESSING", "WORD_COUNT");
            
            // 增加处理计数
            processCounter.increment(1);
            
            // 分割单词并计数
            String[] words = sentence.trim().split("\\s+");
            int wordCount = words.length;
            
            // 增加单词计数
            wordCounter.increment(wordCount);
            
            return "Processed " + wordCount + " words";
        } catch (Exception e) {
            throw new UDFException("Error processing sentence: " + e.getMessage(), e);
        }
    }
    
    // 定义枚举类型的计数器
    enum MyCounter {
        PROCESS_COUNT,    // 处理次数计数器
        ERROR_COUNT       // 错误次数计数器
    }
}
```

## ExecutionContext 详解

ExecutionContext 提供了 UDF 运行时的上下文信息，包括以下主要功能：

### 1. 获取运行时信息
- `getInstanceID()`: 获取 ODPS Instance ID
- `getStageID()`: 获取运行时 StageID
- `getWorkerID()`: 获取运行时 WorkerID
- `getRunningProject()`: 获取运行时的 Project 名
- `getRetryCount()`: 获取运行时当前 Worker 的重试次数
- `getBackupWorkerID()`: 获取当前的 backupWorkerID
- `getTableInfo()`: 获取运行时处理的数据块所属的表或分区信息

### 2. 计数器管理
ExecutionContext 提供了两种方式获取计数器：

#### 通过枚举获取计数器
```java
// 定义枚举类型的计数器
enum MyCounter {
    PROCESS_COUNT,    // 处理次数计数器
    ERROR_COUNT       // 错误次数计数器
}

// 在 UDF 中获取计数器
Counter processCounter = ctx.getCounter(MyCounter.PROCESS_COUNT);
```

#### 通过组名和计数器名获取计数器
```java
// 通过组名和计数器名获取计数器
Counter wordCounter = ctx.getCounter("WORD_PROCESSING", "WORD_COUNT");
```

#### 计数器使用示例
```java
// 增加计数器值
processCounter.increment(1);
wordCounter.increment(wordCount);

// 获取计数器当前值
long count = processCounter.getValue();
```

#### 计数器限制和约束
- 计数器信息最后会汇总在 SQL Task 的 Summary 中
- 可以通过枚举方式或字符串方式定义计数器
- 计数器名称和组名不能为 null 或空字符串
- 计数器名称和组名不能包含 '#' 字符
- 计数器名称和组名加起来长度不能超过 100 字节
- 最多可以定义 64 个用户自定义计数器

### 3. 资源访问
ExecutionContext 提供了多种方法来访问资源：

#### 读取文件类型资源（流式）
```java
// 读取文件类型资源，返回一个带缓存的输入流
BufferedInputStream inputStream = ctx.readResourceFileAsStream("my_resource");
```

#### 读取文件类型资源（一次性加载）
```java
// 读取文件类型资源，一次全部读取到内存，返回 byte[]
byte[] fileContent = ctx.readResourceFile("my_resource");
```

#### 读取压缩档案类型资源
```java
// 读取压缩档案类型资源，返回 BufferedInputStream 的迭代器
Iterable<BufferedInputStream> archiveStreams = ctx.readCacheArchiveAsStream("my_archive_resource");
```

### 4. 其他功能
- `claimAlive()`: 防止 worker 因超时被强制终止。当 UDF 涉及计算密集型的任务时，可能一次调用需要消耗更多时间，调用此方法可以重置时间限制，防止被提前终止。

## 打包和部署

### 1. 编译和打包

在项目根目录下执行 Maven 命令：

```bash
mvn clean package
```

这将生成一个包含所有依赖的 JAR 文件，通常位于 `target/` 目录下。

### 2. 上传 JAR 文件作为资源

打包完成后，需要将 JAR 文件上传到 MaxCompute 作为资源：

```java
// 上传 JAR 文件作为资源
String resourceName = "odps-udf-example.jar";
String localJarPath = "target/odps-udf-example.jar";

FileResource resource = new FileResource();
resource.setName(resourceName);
resource.setComment("UDF functions for character counting");

try (InputStream inputStream = new FileInputStream(localJarPath)) {
    odps.resources().create("project_name", resource, inputStream);
    System.out.println("Resource uploaded successfully: " + resourceName);
} catch (Exception e) {
    System.err.println("Failed to upload resource: " + e.getMessage());
}
```

也可以通过 odpscmd 命令上传：

```sql
ADD JAR odps-udf-example.jar;
```

## 函数注册和使用

### 1. 注册函数

上传资源后，需要注册函数：

```java
// 注册函数
Function function = new Function();
function.setName("character_count");
function.setClassPath("com.example.udf.CharacterCount");
function.setResources(Arrays.asList("odps-udf-example.jar"));

try {
    odps.functions().create("project_name", function);
    System.out.println("Function registered successfully: character_count");
} catch (Exception e) {
    System.err.println("Failed to register function: " + e.getMessage());
}
```

也可以通过 odpscmd 命令注册：

```sql
CREATE FUNCTION character_count AS 'com.example.udf.CharacterCount' 
USING 'odps-udf-example.jar';
```

### 2. 更新函数

使用 Java SDK 更新函数：

```java
// 更新函数
Function updatedFunction = new Function();
updatedFunction.setName("character_count");
updatedFunction.setClassPath("com.example.udf.CharacterCount");
updatedFunction.setResources(Arrays.asList("odps-udf-example-v2.jar"));

try {
    odps.functions().update("project_name", updatedFunction);
    System.out.println("Function updated successfully: character_count");
} catch (Exception e) {
    System.err.println("Failed to update function: " + e.getMessage());
}
```

也可以通过 odpscmd 命令更新：

```sql
DROP FUNCTION character_count;
CREATE FUNCTION character_count AS 'com.example.udf.CharacterCount' 
USING 'odps-udf-example-v2.jar';
```

### 3. 删除函数

使用 Java SDK 删除函数：

```java
// 删除函数
try {
    odps.functions().delete("project_name", "character_count");
    System.out.println("Function deleted successfully: character_count");
} catch (Exception e) {
    System.err.println("Failed to delete function: " + e.getMessage());
}
```

也可以通过 odpscmd 命令删除：

```sql
DROP FUNCTION character_count;
```

### 4. 在 SQL 中使用函数

注册成功后，就可以在 SQL 查询中使用函数了：

```sql
-- 使用自定义函数
SELECT 
    name,
    character_count(name, 'a') AS count_of_a
FROM user_table;

-- 更复杂的使用示例
SELECT 
    sentence,
    character_count(sentence, 'e') AS count_of_e,
    character_count(sentence, ' ') AS count_of_spaces
FROM text_table
WHERE character_count(sentence, 'e') > 5;
```