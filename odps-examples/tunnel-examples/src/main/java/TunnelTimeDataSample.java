/**
 * Created by zhenhong.gzh on 23/5/04.
 */

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;


public class TunnelTimeDataSample {

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

      // ---------- Upload Data ---------------
      // create upload session for table
      // the table schema is {"col0": DATE, "col1": DATETIME, "col2": TIMESTAMP}
      UploadSession uploadSession = tunnel.createUploadSession(project, table, partitionSpec);
      // get table schema
      TableSchema schema = uploadSession.getSchema();

      // open record writer
      RecordWriter recordWriter = uploadSession.openRecordWriter(0);
      ArrayRecord record = (ArrayRecord) uploadSession.newRecord();

      // prepare data
      String dateData = "2023-05-01"; //DATE test data
      ZoneId utcZoneID = ZoneId.of("UTC");
      DateTimeFormatter
          dateFormatter =
          DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(utcZoneID);
      LocalDate localDate = LocalDate.parse(dateData, dateFormatter);

      String datetimeData = "2023-05-01 12:00:00"; //DATETIME test data
      ZoneId localZoneID = ZoneId.of("Asia/Shanghai");
      DateTimeFormatter
          dateTimeFormatter =
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(
              localZoneID);
      ZonedDateTime zonedDateTime = ZonedDateTime.parse(datetimeData, dateTimeFormatter);

      String timestampData = "2023-05-01 00:00:00.12345"; //TIMESTAMP test data
      DateTimeFormatter
          timestampFormatter =
          new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
              .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
      timestampFormatter = timestampFormatter.withZone(utcZoneID);
      Instant time = ZonedDateTime.parse(timestampData, timestampFormatter).toInstant();

      // set to record
      record.setDateAsLocalDate(0, localDate);
      record.setDatetimeAsZonedDateTime(1, zonedDateTime);
      record.setTimestampAsInstant(2, time);

      // write the record
      recordWriter.write(record);

      // close writer
      recordWriter.close();

      // commit uploadSession, the upload finish
      uploadSession.commit(new Long[]{0L});
      System.out.println("upload success!");

      // ---------- Download Data ---------------
      // create download session for table
      // the table schema is {"col0": DATE, "col1": DATETIME, "col2": TIMESTAMP}
      DownloadSession downloadSession = tunnel.createDownloadSession(project, table, partitionSpec);
      schema = downloadSession.getSchema();
      // open record reader, read one record here for example
      RecordReader recordReader = downloadSession.openRecordReader(0, 1);

      // read the record
      ArrayRecord record1 = (ArrayRecord) recordReader.read();

      // get date field data
      LocalDate field0 = record1.getDateAsLocalDate(0);

      // get datetime field data
      ZonedDateTime field1 = record1.getDatetimeAsZonedDateTime(1);

      // get timestamp field data
      Instant field2 = record1.getTimestampAsInstant(2);

      System.out.println("download success!");
      System.out.println(field0);
      System.out.println(field1);
      System.out.println(field2);

    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
