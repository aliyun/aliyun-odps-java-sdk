import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;

/**
 * Created by zhenhong.gzh on 16/7/13.
 */


class UploadThread implements Callable<Boolean> {

  private long id;
  private RecordWriter recordWriter;
  private Record record;
  private TableSchema tableSchema;

  public UploadThread(long id, RecordWriter recordWriter, Record record,
                      TableSchema tableSchema) {
    this.id = id;
    this.recordWriter = recordWriter;
    this.record = record;
    this.tableSchema = tableSchema;
  }

  public Boolean call() {
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

    // write 10 record in each thread
    try {
      for (int i = 0; i < 10; i++) {
        try {
          recordWriter.write(record);
        } catch (IOException e) {
          recordWriter.close();
          e.printStackTrace();
          return false;
        }
      }
      recordWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
}
