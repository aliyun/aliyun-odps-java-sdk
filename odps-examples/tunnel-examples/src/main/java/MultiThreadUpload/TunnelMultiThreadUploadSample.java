/**
 * Created by zhenhong.gzh on 16/7/13.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;

public class TunnelMultiThreadUploadSample {

  private static String accessId = "<your access id>";
  private static String accessKey = "<your access Key>";
  private static String odpsUrl = "<your odps endpoint>";
  private static String project = "<your project>";
  private static String table = "<your table name>";

  // partitions of a partitioned table, eg: "pt=\'1\',ds=\'2\'"
  // if the table is not a partitioned table, do not need it
  private static String partition = "<your partition spec>";

  // the thread number
  private static int threadNum = 10;

  public static void main(String args[]) {
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setEndpoint(odpsUrl);
    odps.setDefaultProject(project);

    try {
      TableTunnel tunnel = new TableTunnel(odps);

      // if the table is not a partitioned table, do not need it
      PartitionSpec partitionSpec = new PartitionSpec(partition);

      // create the upload session for multi threads
      UploadSession uploadSession = tunnel.createUploadSession(project,
                                                               table, partitionSpec);
      System.out.println("Session Status is : "
                         + uploadSession.getStatus().toString());

      // create thread pool
      ExecutorService pool = Executors.newFixedThreadPool(threadNum);
      ArrayList<Callable<Boolean>> callers = new ArrayList<Callable<Boolean>>();

      // create record writer for each thread
      for (int i = 0; i < threadNum; i++) {
        RecordWriter recordWriter = uploadSession.openRecordWriter(i);
        Record record = uploadSession.newRecord();
        callers.add(new UploadThread(i, recordWriter, record,
                                     uploadSession.getSchema()));
      }
      pool.invokeAll(callers);
      pool.shutdown();

      Long[] blockList = new Long[threadNum];
      for (int i = 0; i < threadNum; i++)
        blockList[i] = Long.valueOf(i);

      // commit all the record block list
      uploadSession.commit(blockList);
      System.out.println("upload success!");
    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
