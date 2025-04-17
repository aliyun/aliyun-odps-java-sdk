package com.aliyun.odps.data;

import java.io.IOException;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import com.aliyun.odps.table.utils.Preconditions;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ArrowReaderWrapper implements ArrowRecordReader {

  private ArrowReader arrowReader;

  public ArrowReaderWrapper(ArrowReader arrowReader) {
    Preconditions.checkNotNull(arrowReader, "arrowReader is null");
    this.arrowReader = arrowReader;
  }

  @Override
  public VectorSchemaRoot read() throws IOException {
    while (arrowReader.loadNextBatch()) {
      VectorSchemaRoot vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
      if (vectorSchemaRoot.getRowCount() != 0) {
        return vectorSchemaRoot;
      }
    }
    return null;
  }

  @Override
  public long bytesRead() {
    return arrowReader.bytesRead();
  }

  @Override
  public void close() throws IOException {
    arrowReader.close();
  }
}
