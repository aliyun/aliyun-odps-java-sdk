package com.aliyun.odps.udf.example.stdout;

import com.aliyun.odps.NotImplementedException;
import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.OdpsStorageHandler;
import com.aliyun.odps.udf.Outputer;

/**
 *
 */
public class StdoutStorageHandler extends OdpsStorageHandler{
  @Override
  public Class<? extends Extractor> getExtractorClass() {
    throw new NotImplementedException("not implemented");
  }

  @Override
  public Class<? extends Outputer> getOutputerClass() {
    return StdoutOutputer.class;
  }
}
