/**
 *  Created by zhenhong.gzh on 16/7/13.
 **/

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;

import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;

class BufferedWriterThread implements Callable<Boolean> {

  private UploadSession session;

  public BufferedWriterThread(UploadSession session) {
    this.session = session;
  }

  public Boolean call() {
    TableSchema tableSchema = session.getSchema();

    Record record = session.newRecord();

    for (int i = 0; i < tableSchema.getColumns().size(); i++) {
      Column column = tableSchema.getColumn(i);
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

    try {
      // open buffered writer
      RecordWriter writer = session.openBufferedWriter();

      // write 10 records
      for (int i = 0; i < 10; i++) {
        writer.write(record);
      }

      writer.close();

      return true;

    } catch (TunnelException e) {
      e.printStackTrace();
      return false;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }
}
