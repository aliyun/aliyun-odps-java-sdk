package com.aliyun.odps.udf.example.filesoutput;

import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.OdpsStorageHandler;
import com.aliyun.odps.udf.Outputer;

/**
 *
 */
public class MultiFileStorageHandler extends OdpsStorageHandler {
  @Override
  public Class<? extends Extractor> getExtractorClass() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Class<? extends Outputer> getOutputerClass() {
    return MultiFileOutputer.class;
  }
}
