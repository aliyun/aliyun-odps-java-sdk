package com.aliyun.odps.udf.example.stdout;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.OutputStreamSet;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Outputer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The StdoutOutputer servers as a trivial example to show that outputer is a general interface for exporting records
 * from ODPS. The outputer interface provides flexible interface to write to literally *any* external storage, which
 * does not even have to be file-system based. While Stdout is used as an alternative to OSS/local file system,
 * the storage sink can very well be hbase, hdfs, MySql etc., as long as the corresponding outputer is implemented
 * and connection to external storage can be established.
 *
 * In this case, the outputStreamset will be null, and the location of the external storage of choice can be obtained by
 * DataAttributes.getCustomizedDataLocation()
 */
public class StdoutOutputer extends Outputer{
  private int batchCapacity = 200;
  private ArrayList<String> batchOutputs;


  @Override
  public void setup(ExecutionContext ctx, OutputStreamSet outputStreamSet, DataAttributes attributes) throws IOException {
    assert(outputStreamSet == null);
    this.batchOutputs = new ArrayList<String>(batchCapacity);
    System.out.println("Setting up StdoutOutputer, the supplied customized location is: "
        + attributes.getCustomizedDataLocation());
  }

  @Override
  public void output(Record record) throws IOException {
    StringBuilder b = new StringBuilder();
    b.append("[");
    Column[] columns = record.getColumns();
    for (int i = 0; i < columns.length; i++){
      b.append(record.get(i) + (i == columns.length - 1 ? "" : ","));
    }
    b.append("]");
    String row = b.toString();
    batchOutputs.add(row);
    if (batchOutputs.size() >= batchCapacity) {
      outputBatch();
    }
  }

  @Override
  public void close() throws IOException {
    if (this.batchOutputs.size() > 0) {
      outputBatch();
    }
    System.out.println("All done.");
  }

  private void outputBatch(){
    for (String put : batchOutputs) {
      System.out.println(put);
    }
    this.batchOutputs.clear();
  }
}
