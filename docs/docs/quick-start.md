---
title: 快速开始
sidebar_position: 2
---

# MaxCompute Java SDK 快速开始

MaxCompute的Java SDK为开发者提供了丰富的Java编程语言接口，允许通过Java代码操作和管理MaxCompute服务，包括但不限于项目访问管理、数据表操作、数据传输以及函数管理等。

## Maven 安装

要在您的Maven项目中使用ODPS SDK for Java，您需要将以下依赖添加到项目的`pom.xml`文件中：

```xml
<!-- 添加ODPS SDK for Java的依赖项 -->
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-core</artifactId>
    <version>0.48.8-public</version>
</dependency>
```
请确保您使用的是[Maven Central Repository](https://mvnrepository.com/artifact/com.aliyun.odps/odps-sdk-core)或[阿里云Maven仓库](https://developer.aliyun.com/mvn/)中可用的最新稳定版本。


## 预览表内容

以下代码展示了如何使用MaxCompute Java SDK预览表中的内容：

```java
/**
 * 这是一个演示如何使用MaxCompute Java SDK从表中读取数据的程序。
 */
import com.aliyun.odps.Account;
import com.aliyun.odps.AliyunAccount;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

public class Example {

    public static void main(String[] args) throws OdpsException {
        // 替换为您的阿里云AccessKey ID和AccessKey Secret
        String accessKeyId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
        String accessKeySecret = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
        // 使用AliyunAccessKey账户信息创建账号对象
        Account account = new AliyunAccount(accessKeyId, accessKeySecret);

        // 创建Odps对象并设置Endpoint URL
        Odps odps = new Odps(account);
        String odpsUrl = "<您的Endpoint URL>"; // 替换为您实际的Endpoint URL
        odps.setEndpoint(odpsUrl);
        // 设置默认项目名称
        odps.setDefaultProject("my_project");

        // 获取指定项目和表的实例
        Table table = odps.tables().get("my_project", "my_table");
        
        // 从表中读取前10条记录
        RecordReader reader = table.read(10);
        Record record;
        while ((record = reader.read()) != null) {
            // 将记录转换为字符串并打印
            System.out.println(recordToString(record));
        }
    }

    private static String recordToString(Record record) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < record.getColumnCount(); i++) {
            Object data = record.get(i);
            // 将字节数组转换为字符串，处理二进制字段
            if (data instanceof byte[]) {
                sb.append(new String((byte[]) data));
            } else {
                sb.append(data != null ? data.toString() : "null").append("\t");
            }
        }
        return sb.toString().trim();
    }
}


```

## 代码拆解
以下是对主要代码部分的解释和讲解：

### 基于Account创建ODPS实例
Account 是用于身份验证的对象，您需要使用有效的阿里云AccessKey ID和AccessKey Secret来创建一个AliyunAccount 实例。这两个值用于标识和验证用户的身份，并提供对阿里云服务的访问权限。
除了最基础的使用AK、SK登录的Aliyun Account外，还支持使用Sts Token方式构建ODPS实例，具体可见[Account](api-reference/Account.md)
```java
Account account = new AliyunAccount(accessKeyId, accessKeySecret);
Odps odps = new Odps(account);
```
### 基于ODPS示例获取Table实例
通过ODPS实例，您可以访问MaxCompute项目空间下的所有资源。以下代码获取了一个特定表的实例：
```java
Table table = odps.tables().get("my_project", "my_table");
```
在获取到Table实例后，即可可以进行像读取表元信息，读取表数据等多种行为。具体可参考[Table](api-reference/Table.md)

### 读取表内容
可以使用Table.read()方法对表中数据进行预览，MaxCompute Java SDK还提供了更多的方式来控制数据预览的行为。具体可参考[数据预览](example-code/table-read.md)
```java
// 读取10行数据
RecordReader reader = table.read(10);
Record record;
while ((record = reader.read()) != null) {
    // 处理每条记录
    System.out.println(recordToString(record));
}
```
recordToString 方法将每条记录的字段转换为字符串，对于二进制字段，它将字节数组转换为字符串表示形式。这样您就可以方便地查看和验证表中的数据。

以上就是MaxCompute Java SDK的一个快速开始示例，帮助您在Maven项目中配置SDK，并展示了如何预览MaxCompute表中的内容。此代码示例可以直接用于您的Java开发环境，记得替换所有必要的占位符以适配您的设置。