# Upload Data Example Code

This is an example Java project demonstrates how to upload data to ODPS table.

The program will read a input file, treat each 100 bytes as a record and upload records into specified ODPS table.

## Compilation

### Requirement

- java 1.6+
- maven 3.2.5+

### Get source code and compile

```
git clone git@github.com:aliyun/aliyun-odps-java-sdk.git
cd aliyun-odps-java-sdk
mvn clean package
```

Compiled jar will be found at target/cloudsort-upload-1.0-jar-with-dependencies.jar

## Upload

### Requirement

Download odpscmd at http://repo.aliyun.com/download/odpscmd/0.23.1/odpscmd_public.zip , unzip it,
and edit conf/odps_config.ini.

Run `bin/odpscmd -e "whoami;"` will print message as follows if configuration is correct.

```
Name: ALIYUN$xxxx@aliyun.com
End_Point: https://service.odps.aliyun.com/api
Tunnel_End_Point: https://dt.odps.aliyun.com
Project: xxxxxxxx
```

## Usage

`java -classpath target/cloudsort-upload-1.0-jar-with-dependencies.jar CloudSortUploadExample inputfile accessid accesskey project table [partition]`

### Simple Table

Run `bin/odpscmd -e "create table foo (s string);"` to create target table.

Run `java -classpath target/cloudsort-upload-1.0-jar-with-dependencies.jar CloudSortUploadExample 2records.txt <accessid> <accesskey> <project> foo` to upload 2records.txt to table foo.

### Partitioned Table

Run `bin/odpscmd -e "create table foo (s string) partitoned by (p string)";` to create target table.

Run `bin/odpscmd -e "alter table foo add partition (p=bar)";` to add target partition.

Run `java -classpath target/cloudsort-upload-1.0-jar-with-dependencies.jar CloudSortUploadExample 2records.txt <accessid> <accesskey> <project> foo p=bar` to upload 2records.txt to partition p=bar of table foo.

### Check uploaded data

Run `bin/odpscmd -e "select * from foo"` to check uploaded data.
