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
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;

public class TunnelUploadSample {

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

    try {
      TableTunnel tunnel = new TableTunnel(odps);
      PartitionSpec partitionSpec = new PartitionSpec(partition);

      // create upload session for table
      UploadSession uploadSession = tunnel.createUploadSession(project,
                                                               table, partitionSpec);
      // get table schema
      TableSchema schema = uploadSession.getSchema();

      // open record writer
      RecordWriter recordWriter = uploadSession.openRecordWriter(0);
      Record record = uploadSession.newRecord();

      // set record data
      for (int i = 0; i < schema.getColumns().size(); i++) {
        Column column = schema.getColumn(i);
        switch (column.getType()) {
          case BIGINT:
            record.setBigint(i, 1L);
            break;
          case BOOLEAN:
            record.setBoolean(i, true);
            break;
          case DATETIME:
            record.setDatetime(i, new Date());
            break;
          case DOUBLE:
            record.setDouble(i, 0.0);
            break;
          case STRING:
            record.setString(i, "sample");
            break;
          default:
            throw new RuntimeException("Unknown column type: "
                                       + column.getType());
        }
      }

      // write 10 records
      for (int i = 0; i < 10; i++) {
        recordWriter.write(record);
      }

      // close writer
      recordWriter.close();

      // commit uploadSession, the upload finish
      uploadSession.commit(new Long[]{0L});
      System.out.println("upload success!");
    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
