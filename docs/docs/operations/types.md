---
title: MaxCompute 类型映射
sidebar_position: 2
---

## MaxCompute 类型与 Java 类型映射关系

| ODPS 类型                     | Java 类型                          |
|:----------------------------|:---------------------------------|
| `TINYINT`                   | `java.lang.Byte`                 |
| `SMALLINT`                  | `java.lang.Short`                |
| `INT`                       | `java.lang.Integer`              |
| `BIGINT`                    | `java.lang.Long`                 |
| `BINARY`                    | `com.aliyun.odps.data.Binary`    |
| `FLOAT`                     | `java.lang.Float`                |
| `DOUBLE`                    | `java.lang.Double`               |
| `DECIMAL(precision, scale)` | `java.math.BigDecimal`           |
| `VARCHAR(n)`                | `com.aliyun.odps.data.Varchar`   |
| `CHAR(n)`                   | `com.aliyun.odps.data.Char`      |
| `STRING`                    | `java.lang.String`               |
| `DATE`                      | `java.time.LocalDate`            |
| `DATETIME`                  | `java.time.ZonedDateTime`        |
| `TIMESTAMP`                 | `java.time.Instant`              |
| `BOOLEAN`                   | `java.lang.Boolean`              |
| `TIMESTAMP_NTZ`             | `java.time.LocalDateTime`        |
| `ARRAY<T>`                  | `java.util.List<T>`              |
| `MAP<K, V>`                 | `java.util.Map<K, V>`            |
| `STRUCT<f1:T1, ...>`        | `com.aliyun.odps.data.Struct`    |
| `JSON`                      | `com.aliyun.odps.data.JsonValue` |


## MaxCompute Type 到 Arrow Type 映射表

| MaxCompute Type | Arrow Type (`ValueVector`) | 备注 (Notes)                                                  |
|:----------------|:---------------------------|:------------------------------------------------------------|
| **基础类型**        |                            |                                                             |
| `BOOLEAN`       | `BitVector`                |                                                             |
| `TINYINT`       | `TinyIntVector`            |                                                             |
| `SMALLINT`      | `SmallIntVector`           |                                                             |
| `INT`           | `IntVector`                |                                                             |
| `BIGINT`        | `BigIntVector`             |                                                             |
| `FLOAT`         | `Float4Vector`             |                                                             |
| `DOUBLE`        | `Float8Vector`             |                                                             |
| `DECIMAL`       | `DecimalVector`            | 在 `isExtension` 模式下，，为支持更高精度，可能映射到 `FixedSizeBinaryVector`。 |
| **字符串与二进制类型**   |                            |                                                             |
| `STRING`        | `VarCharVector`            |                                                             |
| `VARCHAR`       | `VarCharVector`            |                                                             |
| `CHAR`          | `VarCharVector`            |                                                             |
| `JSON`          | `VarCharVector`            | `JSON` 类型在 Arrow 层面被当作字符串处理。                                |
| `BINARY`        | `VarBinaryVector`          |                                                             |
| **日期与时间类型**     |                            |                                                             |
| `DATE`          | `DateDayVector`            | 表示自 epoch 以来的天数。                                            |
| `DATETIME`      | `TimeStampVector`          |                                                             |
| `TIMESTAMP`     | `TimeStampVector`          | 在 `isExtension` 模式下，为支持更高精度，会映射到 `StructVector`。            |
| `TIMESTAMP_NTZ` | `TimeStampVector`          | 在 `isExtension` 模式下，为支持更高精度，会映射到 `StructVector`。            |
| **复杂类型**        |                            |                                                             |
| `ARRAY`         | `ListVector`               |                                                             |
| `MAP`           | `MapVector`                |                                                             |
| `STRUCT`        | `StructVector`             |                                                             |

注：isExtension 仅在 preview 接口中被使用