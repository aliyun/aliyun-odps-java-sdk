# ODPS SDK for Java Developers

[![Build Status](https://travis-ci.org/aliyun/aliyun-odps-java-sdk.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-odps-java-sdk)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aliyun.odps/odps/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.aliyun.odps/odps)
[![Javadocs](http://www.javadoc.io/badge/com.aliyun.odps/odps-sdk-core.svg)](http://www.javadoc.io/doc/com.aliyun.odps/odps-sdk-core)


## Requirements

- Java 8+

## Build

```shell
git clone ...
cd aliyun-odps-java-sdk
mvn clean package -DskipTests
```

## Run Unittest

- you will have to configure there test.conf files in source tree:

```
odps-sdk-impl/odps-common-local/src/test/resources/test.conf
odps-sdk-impl/odps-mapred-local/src/test/resources/test.conf
odps-sdk-impl/odps-graph-local/src/test/resources/test.conf
odps-sdk/odps-sdk-lot/src/main/java/com/aliyun/odps/lot/test/resources/test.conf
odps-sdk/odps-sdk-core/src/test/resources/test.conf
```

- in test.conf, set testMode to online, and set debugEnabled to false, and leave domain.\*, taobao.\* to empty
- `mvn clean test`

## Example

```java
Account account = new AliyunAccount("YOUR_ACCESS_ID", "YOUR_ACCESS_KEY");

Odps odps = new Odps(account);

// optional, the default endpoint is
odps.setEndpoint("http://service.odps.aliyun.com/api");
odps.setDefaultProject("YOUR_PROJECT_NAME");

for (Table t : odps.tables()) {
  System.out.println(t.getName());
}
```

## Authors && Contributors

- [Wang Shenggong](https://github.com/shellc)
- [Ni Zheming](https://github.com/nizheming)
- [Li Ruibo](https://github.com/lyman)
- [Guo Zhenhong](https://github.com/guozhenhong)
- [Zhong Wang](https://github.com/cornmonster)
## License

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
