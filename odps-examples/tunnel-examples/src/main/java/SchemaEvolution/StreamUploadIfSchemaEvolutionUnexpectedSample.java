package SchemaEvolution;

import java.util.concurrent.TimeUnit;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.exceptions.SchemaMismatchException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.utils.StringUtils;

/**
 * StreamingUploadExample demonstrates how to upload data to ODPS using streaming upload functionality.
 * <p>
 * The response scenario is while Client A is continuously writing, Client B triggers Schema Evolution,
 * and upstream data uses the original Schema (actually an unintended situation, such as a user executing a DDL task).
 * <p>
 * This example uses Access Key authentication.
 * <p>
 * Note: Make sure to replace the placeholders with your actual ODPS credentials and parameters.
 * <p>
 * Available only in odps-sdk >= 0.51.0
 */
public class StreamUploadIfSchemaEvolutionUnexpectedSample {

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
      // If we have a data import segment, we continuously write data to the table.
      Thread dataWritingThread = new Thread(() -> {
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
          // loop 30 times
          for (int j = 0; j < 30; j++) {
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
            try {
              // Flush the record pack to upload all records at once
              // When it is found that the table schema has changed, this method will throw the exception.
              recordPack.flush();
              System.out.println("flush success");
            } catch (SchemaMismatchException sme) {
              // If the schema has changed, user will get the SchemaMismatchException
              // FIXME: User intervention is actually required here, and the sample code demonstrates how to resume writing
              // The user should re-create the session using the new schema version at this time. In the example, we continue the loop.
              String newSchemaVersion = sme.getLatestSchemaVersion();
              session = rebuildSessionUtilSchemaEvolution(odps, newSchemaVersion);
              recordPack = session.newRecordPack();
            } catch (Exception e) {
              e.printStackTrace();
              break;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      // At the same time, if another user changes the schema of the table...
      Thread schemaEvolutionThread = new Thread(() -> {
        try {
          TimeUnit.SECONDS.sleep(3);
          triggerSchemaEvolution();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      dataWritingThread.start();
      schemaEvolutionThread.start();
      dataWritingThread.join();
      schemaEvolutionThread.join();
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
      } while (!odps.tables().get(project, table).getSchema()
          .basicallyEquals(session.getSchema()));
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
}
