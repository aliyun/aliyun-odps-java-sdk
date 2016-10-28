/**
 * Created by zhenhong.gzh on 16/7/13.
 */

import java.io.IOException;
import java.util.Date;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class TunnelDownloadSample {

  private static String accessId = "<your access id>";
  private static String accessKey = "<your access Key>";
  private static String odpsUrl = "<your odps endpoint>";
  private static String project = "<your project>";

  private static String table = "<your table name>";

  // partitions of a partitioned table, eg: "pt=\'1\',ds=\'2\'"
  // if the table is not a partitioned table, do not need it
  private static String partition = "<your partition spec>";

  public static void main(String args[]) {
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setEndpoint(odpsUrl);
    odps.setDefaultProject(project);
    TableTunnel tunnel = new TableTunnel(odps);

    // if the table is not a partitioned table, do not need it
    PartitionSpec partitionSpec = new PartitionSpec(partition);

    try {
      DownloadSession downloadSession = tunnel.createDownloadSession(project, table,
                                                                     partitionSpec);
      System.out.println("Session Status is : "
                         + downloadSession.getStatus().toString());

      // get total record count
      long count = downloadSession.getRecordCount();
      System.out.println("RecordCount is: " + count);

      // open a record reader with specific count
      RecordReader recordReader = downloadSession.openRecordReader(0,
                                                                   count);
      Record record;
      while ((record = recordReader.read()) != null) {
        // get record and consume it
        // the table schema can get from download session
        consumeRecord(record, downloadSession.getSchema());
      }

      // finished
      recordReader.close();
    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }

  // get data here
  private static void consumeRecord(Record record, TableSchema schema) {

    // traverse the columns
    for (int i = 0; i < schema.getColumns().size(); i++) {
      Column column = schema.getColumn(i);

      String colValue = null;
      switch (column.getType()) {
        case BIGINT: {
          Long v = record.getBigint(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case BOOLEAN: {
          Boolean v = record.getBoolean(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case DATETIME: {
          Date v = record.getDatetime(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case DOUBLE: {
          Double v = record.getDouble(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case STRING: {
          String v = record.getString(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        default:
          throw new RuntimeException("Unknown column type: "
                                     + column.getType());
      }

      // deal with the data
      System.out.print(colValue == null ? "null" : colValue);
      System.out.print("\t");
    }
    System.out.println();
  }
}
