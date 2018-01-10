package com.aliyun.odps.udf.example.weather;

import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.OdpsStorageHandler;
import com.aliyun.odps.udf.Outputer;

public class NetCdfStorageHandler extends OdpsStorageHandler {
  @Override
  public Class<? extends Extractor> getExtractorClass() {
    return NetCdfExtractor.class;
  }

  @Override
  public Class<? extends Outputer> getOutputerClass() {
    throw new UnsupportedOperationException();
  }
}
