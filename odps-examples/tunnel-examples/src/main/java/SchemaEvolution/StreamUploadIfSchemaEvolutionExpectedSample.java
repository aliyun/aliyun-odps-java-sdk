package SchemaEvolution;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.utils.StringUtils;

/**
 * StreamingUploadExample demonstrates how to upload data to ODPS using streaming upload functionality.
 * <p>
 * The response scenario is While Client A is continuously writing,
 * Client B triggers Schema Evolution, and upstream data uses the new Schema.
 * <p>
 * This example uses Access Key authentication.
 * <p>
 * Note: Make sure to replace the placeholders with your actual ODPS credentials and parameters.
 * <p>
 * Available only in odps-sdk >= 0.51.0
 */
public class StreamUploadIfSchemaEvolutionExpectedSample {

  // Replace placeholders with your own credentials and parameters
  private static String accessId;            // Your Access ID
  private static String accessKey;          // Your Access Key
  private static String odpsUrl;         // Your ODPS endpoint URL
  private static String project;               // Your ODPS project name
  private static String table;              // The table to upload data
  // Specifying partition if the target table is partitioned
  // If the table is not partitioned, set this to null or an empty string
  private static String partition;

  /**
   * Initializes the ODPS client using Access Key authentication.
   * You can also use STS authentication via {@link StsAccount}.
   *
   * @return the configured ODPS client instance.
   */
  private static Odps getOdps() {
    // Create an AliyunAccount instance with your Access ID and Access Key
    Account account = new AliyunAccount(accessId, accessKey);
    // Initialize ODPS with the account credentials
    Odps odps = new Odps(account);
    // Set the endpoint for the ODPS client
    odps.setEndpoint(odpsUrl);
    // Set the default project for operations
    odps.setDefaultProject(project);
    // Return the configured ODPS client instance.
    return odps;
  }

  public static void main(String[] args) throws Exception {
    // Get the initialized ODPS client
    Odps odps = getOdps();
    createTestTable();
    try {
      // Initialize a stream upload session for the specified table
      TableTunnel.StreamUploadSession session =
          odps.tableTunnel()
              .buildStreamUploadSession(project, table)
              .setPartitionSpec(partition == null ? null : new PartitionSpec(partition))
              .allowSchemaMismatch(false)
              .build();
      // record the schema version
      System.out.println(session.getSchemaVersion());
      // Create a new record pack for batch operations
      TableTunnel.StreamRecordPack recordPack = session.newRecordPack();

      // Loop to create and append multiple records
      for (int i = 0; i < 100; i++) {
        // Create a new record instance for the session
        Record record = session.newRecord();
        // Set values for each column in the record
        record.setBigint("c1", (long) i);      // First column
        record.setBigint("c2", (long) (i * 2)); // Second column
        // Append the record to the record pack for uploading
        recordPack.append(record);
      }
      // Wait 3 seconds so as not to take up too many resources
      TimeUnit.SECONDS.sleep(3);

      // upstream trigger schema evolution, and start send new data to downstream (three column data)
      triggerSchemaEvolution();

      // If the client can detect that schema evolution has occurred, for example, if it gets a 'SchemaChangeEvent',
      // it can directly reconstruct the Session.
      // TODO: rebuildSessionUtilSchemaEvolution(odps, null);

      // Loop to create and append multiple records
      for (int i = 0; i < 100; i++) {
        // Create a new record instance for the session
        Record record = session.newRecord();
        // Set values for each column in the record
        try {
          record.setBigint("c1", (long) i);      // First column
          record.setBigint("c2", (long) (i * 2)); // Second column
          record.setBigint("c3", (long) (i * 2)); // New column
        } catch (IllegalArgumentException e) {
          // IllegalArgumentException will throw when the data is not compatible with the session schema
          // then should rebuild the session
          recordPack.flush();
          session = rebuildSessionUtilSchemaEvolution(odps, null);
          recordPack = session.newRecordPack();
        }
        // Append the record to the record pack for uploading
        recordPack.append(record);
      }
      recordPack.flush();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Any cleanup code can be placed here
      // In this example, there's no specific resource to close, but it's good practice
    }
  }

  private static TableTunnel.StreamUploadSession rebuildSessionUtilSchemaEvolution(Odps odps,
                                                                                   String latestSchemaVersion)
      throws Exception {
    if (!StringUtils.isNullOrEmpty(latestSchemaVersion)) {
      return odps.tableTunnel()
          .buildStreamUploadSession(project, table)
          .setPartitionSpec(partition == null ? null : new PartitionSpec(partition))
          .allowSchemaMismatch(false)
          .setSchemaVersion(latestSchemaVersion)
          .build();
    } else {
      /**
       * If the schema has changed, but no new schema version,
       * (This may occur when the server is not updated and usually does not go to this branch.)
       * the user should re-create the session and check if the session has noticed the new schema.
       */
      TableTunnel.StreamUploadSession session;
      do {
        session =
            odps.tableTunnel()
                .buildStreamUploadSession(project, table)
                .setPartitionSpec(partition == null ? null : new PartitionSpec(partition))
                .allowSchemaMismatch(false)
                .build();
        System.out.println("Session Schema: " + debugString(session.getSchema()));
        System.out.println("Table Schema: " + debugString(odps.tables().get(project, table).getSchema()));
        TimeUnit.SECONDS.sleep(5);
      } while (!basicallyEquals(odps.tables().get(project, table).getSchema()
          , session.getSchema()));
      return session;
    }
  }

  /**
   * Create a test table, with two columns, c1 bigint and c2 bigint
   */
  private static void createTestTable() throws OdpsException {
    getOdps().tables().delete(project, table, true);
    getOdps()
        .tables()
        .newTableCreator(project, table, TableSchema.builder()
            .withBigintColumn("c1")
            .withBigintColumn("c2")
            .build())
        .ifNotExists()
        .debug()
        .create();
  }

  /**
   * Trigger schema evolution on the table. Here we run a SQL statement to add a column.
   */
  private static void triggerSchemaEvolution() throws OdpsException {
    Instance
        instance =
        SQLTask.run(getOdps(), "alter table " + project + "." + table + " add column c3 bigint;");
    // print logview to check the progress of schema evolution
    System.out.println(getOdps().logview().generateLogView(instance, 24));
    instance.waitForSuccess();
  }

  private static String debugString(TableSchema schema) {
    return schema.getAllColumns().stream()
        .map(column -> column.getName() + "(" + column.getTypeInfo().getTypeName() + ")")
        .collect(Collectors.joining(", "));
  }

  /**
   * Check if two schemas are basically equal
   */
  private static boolean basicallyEquals(TableSchema a, TableSchema b) {
    List<Column> columnsA = a.getAllColumns();
    List<Column> columnsB = b.getAllColumns();
    if (columnsA.size() != columnsB.size()) {
      return false;
    }
    for (int i = 0; i < columnsA.size(); i++) {
      Column columnA = columnsA.get(i);
      Column columnB = columnsB.get(i);
      if (!columnA.getName().equals(columnB.getName()) || !columnA.getTypeInfo().getTypeName()
          .equals(columnB.getTypeInfo().getTypeName())) {
        return false;
      }
    }
    return true;
  }
}
