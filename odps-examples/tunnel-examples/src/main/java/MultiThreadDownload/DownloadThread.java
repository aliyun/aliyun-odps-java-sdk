/**
 * Created by zhenhong.gzh on 16/7/13.
 */

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

class DownloadThread implements Callable<Long> {
  private long id;
  private RecordReader recordReader;
  private TableSchema tableSchema;
  public DownloadThread(int id,
                        RecordReader recordReader, TableSchema tableSchema) {
    this.id = id;
    this.recordReader = recordReader;
    this.tableSchema = tableSchema;
  }

  public Long call() {
    Long recordNum = 0L;
    try {
      Record record;
      while ((record = recordReader.read()) != null) {
        recordNum++;
        System.out.print("Thread " + id + "\t");
        consumeRecord(record, tableSchema);
      }
      recordReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return recordNum;
  }
  private static void consumeRecord(Record record, TableSchema schema) {
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
      System.out.print(colValue == null ? "null" : colValue);
      if (i != schema.getColumns().size())
        System.out.print("\t");
    }
    System.out.println();
  }
}
