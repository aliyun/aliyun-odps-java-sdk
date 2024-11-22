import java.time.Instant;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.GenerateExpression;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.google.common.collect.ImmutableMap;

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

    // For example, we now have a Record to be written
    Record record = new ArrayRecord(schema);
    record.set("a", 123L);
    record.set("d", Instant.now());

    // Auto generate partitionSpec from record and schema
    PartitionSpec partitionSpec = schema.generatePartitionSpec(record);
    System.out.println(partitionSpec);
  }

  /**
   * How to manually generate partitionSpec from record and schema
   */
  private static PartitionSpec getPartitionSpec(Record record, TableSchema schema) {
    // And partition spec means where the record will be written to
    PartitionSpec partitionSpec = new PartitionSpec();

    // We iterate over all partition columns to combine partitions
    for (Column column : schema.getPartitionColumns()) {
      GenerateExpression generateExpression = column.getGenerateExpression();
      if (generateExpression != null) {
        System.out.println(generateExpression);  // trunc_time(d, 'day')
        // We can use GenerateExpression to generate partition values from Record
        String ptValue = generateExpression.generate(record);
        // and then combined into actual partition
        partitionSpec.set(column.getName(), ptValue);
      }
    }
    return partitionSpec;
  }
}
