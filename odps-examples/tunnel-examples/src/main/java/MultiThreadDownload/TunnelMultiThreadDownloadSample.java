/**
 * Created by zhenhong.gzh on 16/7/13.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;


/**
 * use thread pool to download data
 */
public class TunnelMultiThreadDownloadSample {
  private static String accessId = "<your access id>";
  private static String accessKey = "<your access Key>";
  private static String odpsUrl = "<your odps endpoint>";
  private static String project = "<your project>";
  private static String table = "<your table name>";

  // partitions of a partitioned table, eg: "pt=\'1\',ds=\'2\'"
  // if the table is not a partitioned table, do not need it
  private static String partition = "<your partition spec>";

  // thread number
  private static int threadNum = 10;

  public static void main(String args[]) {
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setEndpoint(odpsUrl);
    odps.setDefaultProject(project);
    TableTunnel tunnel = new TableTunnel(odps);

    // if the table is not a partitioned table, do not need it
    PartitionSpec partitionSpec = new PartitionSpec(partition);

    DownloadSession downloadSession;
    try {
      // create download session for thread pool
      downloadSession = tunnel.createDownloadSession(project, table,
                                                     partitionSpec);
      System.out.println("Session Status is : "
                         + downloadSession.getStatus().toString());

      long count = downloadSession.getRecordCount();
      System.out.println("RecordCount is: " + count);

      // create thread pool
      ExecutorService pool = Executors.newFixedThreadPool(threadNum);
      ArrayList<Callable<Long>> callers = new ArrayList<Callable<Long>>();

      // split total count
      long step = count / threadNum;
      for (int i = 0; i < threadNum - 1; i++) {

        // open record reader with specific record count for each thread
        // read count is step
        RecordReader recordReader = downloadSession.openRecordReader(
            step * i, step);
        callers.add(new DownloadThread( i, recordReader, downloadSession.getSchema()));
      }

      // the last thread, read count is (count- ((threadNum - 1) * step)
      RecordReader recordReader = downloadSession.openRecordReader(step * (threadNum - 1), count
                                                                                           - ((threadNum - 1) * step));
      callers.add(new DownloadThread( threadNum - 1, recordReader, downloadSession.getSchema()));

      Long downloadNum = 0L;
      // invoke the thread pool to download record
      List<Future<Long>> recordNum = pool.invokeAll(callers);
      for (Future<Long> num : recordNum)
        downloadNum += num.get();

      System.out.println("Record Count is: " + downloadNum);
      pool.shutdown();
    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }
}
