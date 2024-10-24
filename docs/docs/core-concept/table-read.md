---
id: read
title: 数据预览
sidebar_label: 数据预览
sidebar_position: 10
---

# 数据预览

MaxCompute SDK可以使用`Table.read()`方法，对表内数据进行预览。
本接口需要用户具有希望预览列的`select`权限。
SDK提供了多个重载的`read`方法，用于在不同限制条件下，读取表内的数据。以下是这些方法的说明和代码示例：

## 基本读取

读取表数据，最多返回指定数量的记录行数。

```java
/**
 * 读取表内的数据
 * 读取数据时，最多返回 1w 条记录 (project read 默认值），若超过，数据将被截断。<br />
 *
 * @param limit 最多读取的记录行数
 * @return {@link RecordReader}对象
 * @throws OdpsException
 */
public RecordReader read(int limit)throws OdpsException;
```

## 高级读取

读取指定分区或列的数据，并能够控制读取行数。

```java
  /**
 * 读取表内的数据 <br />
 * 读取数据时，最多返回 1w 条记录 (project read 默认值），若超过，数据将被截断。<br />
 *
 * @param partition 表的分区{@link PartitionSpec}。如不指定分区可传入null。
 * @param columns   所要读取的列名的列表。如果读取全表可传入null
 * @param limit     最多读取的记录行数。
 * @return {@link RecordReader}对象
 * @throws OdpsException
 */
public RecordReader read(PartitionSpec partition,List<String> columns,int limit)throws OdpsException;
  ```

## 兼容模式

我们在0.47.0-public 及以后的版本，更新了接口的返回类型。
为了便于老用户进行升级，我们新增了兼容旧Read接口返回类型的模式。
然而，此参数仅是在现有接口的基础上额外进行了类型转换，将导致额外的性能开销，不推荐用户使用。

```java
  /**
 * 读取表内的数据 <br />
 * 读取数据时，最多返回 1w 条记录 (project read 默认值），若超过，数据将被截断。<br />
 *
 * @param partition 表的分区{@link PartitionSpec}。如不指定分区可传入null。
 * @param columns   所要读取的列名的列表。如果读取全表可传入null
 * @param limit     最多读取的记录行数。
 * @param timezone  设置 datetime 类型数据的时区，新接口使用Java8无时区类型，故指定Timezone无效
 * @param useLegacyMode  是否使用兼容旧Read接口模式，默认为false。老接口性能较差，不推荐。
 * @return {@link ArrowStreamRecordReader}
 * @throws OdpsException
 */
public RecordReader read(PartitionSpec partition,List<String> columns,int limit,
    String timezone,boolean useLegacyMode)
    throws OdpsException;
  ```

## 指定TunnelEndpoint

read接口底层使用tunnel endpoint进行读取，通常情况下，tunnel endpoint由SDK自动获取，用户无需关心。
当用户希望指定tunnel endpoint时，可以使用该接口

```java
  /**
 * 读取表内的数据 <br />
 * 读取数据时，最多返回 1w 条记录 (project read 默认值），若超过，数据将被截断。<br />
 *
 * @param partition 表的分区{@link PartitionSpec}。如不指定分区可传入null。
 * @param columns   所要读取的列名的列表。如果读取全表可传入null
 * @param limit     最多读取的记录行数。
 * @param timezone  设置 datetime 类型数据的时区，新接口使用Java8无时区类型，故指定Timezone无效
 * @param useLegacyMode  是否使用兼容旧Read接口模式，默认为false。老接口性能较差，不推荐。
 * @return {@link ArrowStreamRecordReader}
 * @throws OdpsException
 */
public RecordReader read(PartitionSpec partition,List<String> columns,int limit,
    String timezone,boolean useLegacyMode,String tunnelEndpoint)
    throws OdpsException;
```

## RecordReader

对于RecordReader读取到的Record数据，其OdpsType与JavaType的映射关系如下

| OdpsType                 | JavaType                          |
|--------------------------|-----------------------------------|
| TINYINT                  | java.lang.Byte                    |
| SMALLINT                 | java.lang.Short                   |
| INT                      | java.lang.Integer                 |
| BIGINT                   | java.lang.Long                    |
| BINARY                   | com.aliyun.odps.data.Binary       |
| FLOAT                    | java.lang.Float                   |
| DOUBLE                   | java.lang.Double                  |
| DECIMAL(precision,scale) | java.math.BigDecimal              |
| VARCHAR(n)               | com.aliyun.odps.data.Varchar      |
| CHAR(n)                  | com.aliyun.odps.data.Char         |
| STRING                   | java.lang.String                  |
| DATE                     | java.time.LocalDate               |
| DATETIME                 | java.time.ZonedDateTime           |
| TIMESTAMP                | java.time.Instant                 |
| BOOLEAN                  | java.lang.Boolean                 |
| TIMESTAMP_NTZ            | java.time.LocalDateTime           |
| ARRAY                    | java.util.ArrayList               |
| MAP                      | java.util.HashMap                 |
| STRUCT                   | com.aliyun.odps.data.SimpleStruct |
| JSON                     | com.aliyun.odps.data.JsonValue    |

## 注意

- 在 0.47.0-public 及以后的版本，read接口使用TableTunnel的preview方法进行重新实现。与之前版本相比，变化如下
    1. OdpsType与JavaType的映射关系，时间类型使用了Java8推荐的无时区类型。
       |OdpsType | New | Legacy |
       |---------|-----|--------|
       |DATE | java.time.LocalDate | java.sql.Date |
       |DATETIME |java.time.ZonedDateTime | java.util.Date |
       |TIMESTAMP| java.time.Instant | java.sql.Timestamp |

    2. 对报错信息，进行了重新设计，更便于用户理解。

- Read 接口仅适用于数据预览，对于大量数据，推荐使用 TableTunnel
- Read 接口的输出，不保证有序。

