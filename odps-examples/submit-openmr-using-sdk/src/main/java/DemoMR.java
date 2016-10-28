import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.mail.Session;

import com.aliyun.odps.Column;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class DemoMR {

  public static class MyMapper extends MapperBase {
  }

  public static class MyReducer extends ReducerBase {
  }

  public static void main(String[] args) throws Exception {

    String endpoint = "http://service.odps.aliyun.com/api";
    String accessId = "YOUR_ACCESS_ID";
    String accessKey = "YOUR_ACCESS_KEY";
    String project = "YOUR_PROJECT";

    Odps odps = new Odps(new AliyunAccount(accessId, accessKey));
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);

    // upload jar as resource
    System.err.println("uploading odps-demo.jar ...");
    String resName = "odps-demo.jar";
    if (odps.resources().exists(resName)) {
      odps.resources().delete(resName);
    }
    JarResource res = new JarResource();
    res.setName(resName);
    odps.resources().create(res, new FileInputStream("target/odps-demo-1.0.jar"));

    // create input/output table
    System.err.println("creating input table mr_demo_input ...");
    TableSchema inputSchema = new TableSchema();
    inputSchema.addColumn(new Column("s", OdpsType.STRING));
    odps.tables().create("mr_demo_input", inputSchema, true);

    System.err.println("creating output table mr_demo_output ...");
    TableSchema outputSchema = new TableSchema();
    outputSchema.addColumn(new Column("s", OdpsType.STRING));
    outputSchema.addColumn(new Column("i", OdpsType.BIGINT));
    odps.tables().create("mr_demo_output", outputSchema, true);

    // submit mr job
    System.err.println("submitting mr job ...");
    SessionState.get().setOdps(odps);
    SessionState.get().setLocalRun(false);

    JobConf job = new JobConf();
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    InputUtils.addTable(TableInfo.builder().tableName("mr_demo_input").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("mr_demo_output").build(), job);

    job.setMapOutputKeySchema(SchemaUtils.fromString("s:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("s:string,i:bigint"));

    job.setResources(resName);

    RunningJob rj = JobClient.runJob(job);
    rj.waitForCompletion();
  }

}
