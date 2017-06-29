# Submit OpenMR job using Java SDK

This is a tiny example project demonstrates how to submit OpenMR job using Java SDK other than Console(odpscmd).

## Usage

- Modify source code

Edit `src/main/java/DemoMR.java`, fill your access id, access key and project.

- Compile

`mvn clean package`

Generated jar will be placed in `target/odps-demo-1.0.jar`

- Run

First, download [ODPS Console(odpscmd)](https://docs-aliyun.cn-hangzhou.oss.aliyun-inc.com/cn/odps/0.0.90/assets/download/odpscmd_public.zip?spm=5176.doc27991.2.2.NhDlMO&file=odpscmd_public.zip), and unzip it into a folder, say `/opt/odpscmd`. ODPS Console will provide all runtime dependencies with minimal effort (mainly because odps-mapred-bridge.jar can not be found at maven.org yet).

run `java -classpath /opt/odpscmd/lib/*:target/odps-demo-1.0.jar DemoMR`

```
uploading odps-demo.jar ...
creating input table mr_demo_input ...
creating output table mr_demo_output ...
submitting mr job ...
http://logview.odps.aliyun.com/******************************
InstanceId: 20160707045450486gyaqr8jc2
Inputs:
    odpsdemo_dev.mr_demo_input: 0 (0 bytes)
Outputs:
    odpsdemo_dev.mr_demo_output: 0 (0 bytes)
M1_odpsdemo_dev_20160707045450486gyaqr8jc2_LOT_0_0_0_job0:
    Worker Count:1
    Input Records:
        input: 0 (min: 0, max: 0, avg: 0)
    Output Records:
        R2_1: 0 (min: 0, max: 0, avg: 0)
R2_1_odpsdemo_dev_20160707045450486gyaqr8jc2_LOT_0_0_0_job0:
    Worker Count:1
    Input Records:
        input: 0 (min: 0, max: 0, avg: 0)
    Output Records:
        R2_1FS_DataSink_6: 0 (min: 0, max: 0, avg: 0)

User defined counters: 0
OK
```
