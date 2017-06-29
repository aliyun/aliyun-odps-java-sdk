package com.aliyun.odps.udf.example.image;

import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.OdpsStorageHandler;
import com.aliyun.odps.udf.Outputer;

/**
 *
 */
public class ImageStorageHandler extends OdpsStorageHandler {
  @Override
  public Class<? extends Extractor> getExtractorClass() {
    return ImageExtractor.class;
  }

  @Override
  public Class<? extends Outputer> getOutputerClass() {
    return ImageOutputer.class;
  }
}
