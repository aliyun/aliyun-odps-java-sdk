import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * StreamingUploadExample demonstrates how to upload data to ODPS using streaming upload functionality.
 * This example uses Access Key authentication.
 * <p>
 * Note: Make sure to replace the placeholders with your actual ODPS credentials and parameters.
 */
public class TunnelStreamingUploadExample {

  // Replace placeholders with your own credentials and parameters
  private static String accessId = "<your access id>";            // Your Access ID
  private static String accessKey = "<your access Key>";          // Your Access Key
  private static String odpsUrl = "<your odps endpoint>";         // Your ODPS endpoint URL
  private static String project = "<your project>";               // Your ODPS project name
  private static String table = "<your table name>";              // The table to upload data to
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
              .buildStreamUploadSession(odps.getDefaultProject(), table)
              .setPartitionSpec(partition == null ? null : new PartitionSpec(partition))
              .build();

      // Create a new record pack for batch operations
      TableTunnel.StreamRecordPack recordPack = session.newRecordPack();

      // Loop to create and append multiple records
      for (int i = 0; i < 3; i++) {
        // Create a new record instance for the session
        Record record = session.newRecord();
        // Set values for each column in the record
        record.setBigint("c1", (long) i);      // First column
        record.setBigint("c2", (long) (i * 2)); // Second column
        // Append the record to the record pack for uploading
        recordPack.append(record);
      }
      // Flush the record pack to upload all records at once
      recordPack.flush();
      // show table to verify the data
      showTable();

      // Note: When encountering table schema evolution, we should re-create the session to continue upload
      // Additionally, we should wait for a while to ensure the session notice the new schema, here we wait for 1 minutes (10 * 6s)
      triggerSchemaEvolution();

      for (int retry = 0; retry < 10; retry++) {
        session =
            odps.tableTunnel()
                .buildStreamUploadSession(odps.getDefaultProject(), table)
                .setPartitionSpec(partition == null ? null : new PartitionSpec(partition))
                .build();
        if (session.getSchema().getAllColumns().size() == 3) {
          break;
        }
        TimeUnit.SECONDS.sleep(6);
      }

      recordPack = session.newRecordPack();

      for (int i = 0; i < 3; i++) {
        Record record = session.newRecord();
        record.setBigint("c1", (long) i);
        record.setBigint("c2", (long) (i * 2));
        record.setBigint("c3", (long) (i * 3)); // new column
        recordPack.append(record);
      }
      recordPack.flush();
      showTable();

    } catch (TunnelException e) {
      // Handle any exceptions specific to the tunnel operations
      System.err.println("Tunnel Exception: " + e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      // Handle general IO exceptions
      System.err.println("IO Exception: " + e.getMessage());
      e.printStackTrace();
    } finally {
      // Any cleanup code can be placed here
      // In this example, there's no specific resource to close, but it's good practice
    }
  }

  /**
   * Create a test table, with two columns, c1 bigint and c2 bigint
   */
  private static void createTestTable() throws OdpsException {
    getOdps()
        .tables()
        .newTableCreator(table, TableSchema.builder()
            .withBigintColumn("c1")
            .withBigintColumn("c2")
            .build())
        .ifNotExists()
        .create();
  }

  /**
   * Trigger schema evolution on the table. Here we run a SQL statement to add a column.
   */
  private static void triggerSchemaEvolution() throws OdpsException {
    Instance instance = SQLTask.run(getOdps(), "alter table " + table + " add column c3 bigint;");
    instance.waitForSuccess();
  }

  /**
   * Show the table first 10 rows to verify the data.
   */
  private static void showTable() throws OdpsException {
    getOdps().tables().get(table).read(10).forEach(System.out::println);
    System.out.println("\n");
  }
}
