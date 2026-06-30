package com.aliyun.odps.table.read.split;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.task.SQLTask;

/**
 * Integration test that verifies the ordering of splits returned by
 * {@link InputSplitAssigner#getAllSplits()} when using BUCKET split mode
 * on a primary-key (delta) table.
 *
 * <p>This test:
 * <ol>
 *   <li>Creates a delta table with PRIMARY KEY and CLUSTERED BY (hash buckets)</li>
 *   <li>Inserts test data via upsert SQL</li>
 *   <li>Creates a read session with SplitByBucket</li>
 *   <li>Inspects split order from getAllSplits()</li>
 * </ol>
 *
 * <p>Configuration via environment variables:
 * <ul>
 *   <li>ODPS_ACCESS_ID</li>
 *   <li>ODPS_ACCESS_KEY</li>
 *   <li>ODPS_ENDPOINT</li>
 *   <li>ODPS_PROJECT</li>
 *   <li>ODPS_TUNNEL_ENDPOINT (optional)</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   mvn exec:java -Dexec.mainClass="com.aliyun.odps.table.read.split.BucketSplitOrderTest"
 * </pre>
 */
public class BucketSplitOrderTest {

    private static final String TABLE_NAME = "test_bucket_split_order_pk";
    private static final int BUCKET_NUM = 8;
    private static final int ROWS_PER_BUCKET = 10;

    public static void main(String[] args) throws Exception {
        Odps odps = createOdps();
        String project = odps.getDefaultProject();

        System.out.println("=== Bucket Split Order Test (Primary Key Table) ===");
        System.out.println("Project:  " + project);
        System.out.println("Table:    " + TABLE_NAME);
        System.out.println("Buckets:  " + BUCKET_NUM);
        System.out.println();

        // Step 1: Create a primary-key table with hash clustering
        createPrimaryKeyTable(odps);

        // Step 2: Insert data
        insertData(odps);

        // Step 3: Build read session with SplitByBucket
        TableIdentifier tableId = TableIdentifier.of(project, TABLE_NAME);
        EnvironmentSettings settings = buildSettings(odps);

        TableBatchReadSession readSession = new TableReadSessionBuilder()
                .identifier(tableId)
                .withSettings(settings)
                .withSplitOptions(SplitOptions.newBuilder().SplitByBucket().build())
                .withArrowOptions(
                        ArrowOptions.newBuilder()
                                .withDatetimeUnit(ArrowOptions.TimestampUnit.MILLI)
                                .withTimestampUnit(ArrowOptions.TimestampUnit.MICRO)
                                .build())
                .buildBatchReadSession();

        System.out.println("Session ID: " + readSession.getId());

        InputSplitAssigner assigner = readSession.getInputSplitAssigner();
        System.out.println("Assigner class: " + assigner.getClass().getName());
        System.out.println("Splits count:   " + assigner.getSplitsCount());
        System.out.println();

        // Step 4: Inspect split ordering
        InputSplit[] splits = assigner.getAllSplits();

        System.out.println("--- All Splits (in getAllSplits() order) ---");
        System.out.printf("%-6s %-12s %-12s %s%n", "Pos", "SplitIndex", "BucketId", "Class");
        System.out.println("---------------------------------------------------");

        boolean allHaveBucket = true;
        int prevBucketId = -1;
        boolean bucketIdMonotonicallyIncreasing = true;

        for (int i = 0; i < splits.length; i++) {
            InputSplit split = splits[i];
            int splitIndex = -1;
            int bucketId = -1;

            if (split instanceof InputSplitWithIndex) {
                splitIndex = ((InputSplitWithIndex) split).getSplitIndex();
            }
            if (split instanceof InputSplitWithBucket) {
                bucketId = ((InputSplitWithBucket) split).getBucketId();
            } else {
                allHaveBucket = false;
            }

            System.out.printf("%-6d %-12d %-12d %s%n",
                    i, splitIndex, bucketId, split.getClass().getSimpleName());

            if (bucketId >= 0 && prevBucketId >= 0 && bucketId <= prevBucketId) {
                bucketIdMonotonicallyIncreasing = false;
            }
            prevBucketId = bucketId;
        }

        // Summary
        System.out.println();
        System.out.println("=== Analysis ===");
        System.out.println("Total splits:                       " + splits.length);
        System.out.println("All splits implement WithBucket:    " + allHaveBucket);
        System.out.println("BucketId monotonically increasing:  " + bucketIdMonotonicallyIncreasing);

        boolean splitIndexMatchesPosition = true;
        for (int i = 0; i < splits.length; i++) {
            if (splits[i] instanceof InputSplitWithIndex) {
                if (((InputSplitWithIndex) splits[i]).getSplitIndex() != i) {
                    splitIndexMatchesPosition = false;
                    break;
                }
            }
        }
        System.out.println("SplitIndex == array position:       " + splitIndexMatchesPosition);

        boolean bucketIdEqualsSplitIndex = true;
        for (int i = 0; i < splits.length; i++) {
            if (splits[i] instanceof InputSplitWithBucket && splits[i] instanceof InputSplitWithIndex) {
                int si = ((InputSplitWithIndex) splits[i]).getSplitIndex();
                int bi = ((InputSplitWithBucket) splits[i]).getBucketId();
                if (si != bi) {
                    bucketIdEqualsSplitIndex = false;
                    System.out.println("  Mismatch: pos=" + i + " splitIndex=" + si + " bucketId=" + bi);
                }
            }
        }
        System.out.println("BucketId == SplitIndex:             " + bucketIdEqualsSplitIndex);

        System.out.println();
        System.out.println("=== Conclusion ===");
        if (allHaveBucket && bucketIdMonotonicallyIncreasing && bucketIdEqualsSplitIndex) {
            System.out.println("RESULT: Splits are returned in strict bucket order (0, 1, 2, ...)");
        } else if (allHaveBucket && bucketIdMonotonicallyIncreasing) {
            System.out.println("RESULT: BucketIds are monotonically increasing but may not equal splitIndex");
        } else if (allHaveBucket) {
            System.out.println("RESULT: Splits have bucketId but are NOT in bucket order");
        } else {
            System.out.println("RESULT: Splits do NOT implement InputSplitWithBucket — SDK version too old?");
        }

        // Cleanup
        executeSql(odps, "DROP TABLE IF EXISTS " + TABLE_NAME);
        System.out.println("\nTable dropped. Done.");
    }

    private static void createPrimaryKeyTable(Odps odps) throws OdpsException {
        System.out.println("Creating hash-clustered table with " + BUCKET_NUM + " buckets...");

        executeSql(odps, "DROP TABLE IF EXISTS " + TABLE_NAME);

        String createSql = String.format(
                "CREATE TABLE %s ("
                        + "id BIGINT NOT NULL, "
                        + "name STRING, "
                        + "value DOUBLE, "
                        + "PRIMARY KEY(id)"
                        + ") TBLPROPERTIES('transactional'='true') LIFECYCLE 1",
                TABLE_NAME);
        executeSql(odps, createSql);

        System.out.println("Table created: " + TABLE_NAME);
    }

    private static void insertData(Odps odps) throws OdpsException {
        System.out.println("Inserting " + (BUCKET_NUM * ROWS_PER_BUCKET) + " rows...");

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(TABLE_NAME).append(" VALUES ");
        int total = BUCKET_NUM * ROWS_PER_BUCKET;
        for (int i = 0; i < total; i++) {
            if (i > 0) sql.append(",");
            sql.append(String.format("(%d, 'name_%d', %d.%d)", i, i, i, i));
        }
        executeSql(odps, sql.toString());
        System.out.println("Data inserted.");
        System.out.println();
    }

    private static EnvironmentSettings buildSettings(Odps odps) {
        Credentials credentials = Credentials.newBuilder()
                .withAccount(odps.getAccount())
                .build();
        EnvironmentSettings.Builder builder = EnvironmentSettings.newBuilder()
                .inAutoMode()
                .withCredentials(credentials)
                .withServiceEndpoint(odps.getEndpoint());
        if (odps.getTunnelEndpoint() != null && !odps.getTunnelEndpoint().isEmpty()) {
            builder.withTunnelEndpoint(odps.getTunnelEndpoint());
        }
        return builder.build();
    }

    private static Odps createOdps() {
        String accessId = requireEnv("ODPS_ACCESS_ID");
        String accessKey = requireEnv("ODPS_ACCESS_KEY");
        String endpoint = requireEnv("ODPS_ENDPOINT");
        String project = requireEnv("ODPS_PROJECT");
        String tunnelEndpoint = System.getenv("ODPS_TUNNEL_ENDPOINT");

        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(endpoint);
        if (tunnelEndpoint != null && !tunnelEndpoint.isEmpty()) {
            odps.setTunnelEndpoint(tunnelEndpoint);
        }
        return odps;
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            System.err.println("ERROR: Environment variable " + name + " is required.");
            System.exit(1);
        }
        return value;
    }

    private static void executeSql(Odps odps, String sql) throws OdpsException {
        if (!sql.endsWith(";")) {
            sql += ";";
        }
        Instance instance = SQLTask.run(odps, sql);
        instance.waitForSuccess();
    }
}
