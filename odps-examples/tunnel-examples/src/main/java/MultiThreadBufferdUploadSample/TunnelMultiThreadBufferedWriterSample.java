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
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

public class TunnelMultiThreadBufferedWriterSample {
  // 初始化 ODPS 和 tunnel 的代码
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
    TableTunnel tunnel = new TableTunnel(odps);

    // if the table is not a partitioned table, do not need it
    PartitionSpec partitionSpec = new PartitionSpec(partition);

    try {
      TableTunnel.UploadSession uploadSession = tunnel.createUploadSession(project, table, partitionSpec);
      System.out.println("Session Status is : "
                         + uploadSession.getStatus().toString());

      // create thread pool
      ExecutorService pool = Executors.newFixedThreadPool(threadNum);
      ArrayList<Callable<Boolean>> callers = new ArrayList<Callable<Boolean>>();

      for (int i = 0; i < threadNum; i++) {
        // the buffered record writer is in each thread
        callers.add(new BufferedWriterThread(uploadSession));
      }
      pool.invokeAll(callers);
      pool.shutdown();

      uploadSession.commit();
    } catch (TunnelException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }
}
