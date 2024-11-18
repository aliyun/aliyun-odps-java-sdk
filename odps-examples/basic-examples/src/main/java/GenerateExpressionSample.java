import java.time.Instant;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.GenerateExpression;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.task.SQLTask;
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class GenerateExpressionSample {

  private static String accessId = "<your access id>";
  private static String accessKey = "<your access Key>";
  private static String odpsUrl = "<your odps endpoint>";
  private static String project = "<your project>";

  public static void main(String[] args) throws Exception {
    // how to init odps client can be found in OdpsClientSetupSample.java
    Odps odps = OdpsClientSetupSample.buildWithAccessKey(accessId, accessKey);
    odps.setEndpoint(odpsUrl);
    odps.setDefaultProject(project);

    // Create table with auto partitioned by generate expression `trunc_time(d, 'day') as part_value`
    SQLTask.run(odps, odps.getDefaultProject(),
                "create table if not exists auto_pt(a bigint, d timestamp) auto partitioned by (trunc_time(d, 'day') as part_value);",
                ImmutableMap.of("odps.sql.type.system.odps2", "true"),
                null
    ).waitForSuccess();

    // Get table schema and check the generate expression
    TableSchema schema = odps.tables().get("auto_pt").getSchema();
    Column ptColumn = schema.getPartitionColumns().get(0);
    GenerateExpression generateExpression = ptColumn.getGenerateExpression();

    // GenerateExpression is an interface, you can cast it to a specific expression
    TruncTime truncTime = (TruncTime) generateExpression;
    System.out.println(truncTime);

    // For example, we now have a Record to be written
    Record record = new ArrayRecord(schema);
    record.set("a", 123L);
    record.set("d", Instant.now());

    // We can use GenerateExpression to generate partition values from Record
    String ptValue = generateExpression.generate(record);

    // and then combined into actual partition
    PartitionSpec partitionSpec = new PartitionSpec();
    partitionSpec.set(ptColumn.getName(), ptValue);

    System.out.println(partitionSpec);
  }
}
