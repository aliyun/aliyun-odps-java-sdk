package com.aliyun.odps.udf.example.text.withclone;

import com.aliyun.odps.NotImplementedException;
import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.OdpsStorageHandler;
import com.aliyun.odps.udf.Outputer;

public class TextStorageHandlerWithClone extends OdpsStorageHandler {

  @Override
  public Class<? extends Extractor> getExtractorClass() {
    return TextExtractorWithClone.class;
  }

  @Override
  public Class<? extends Outputer> getOutputerClass() {
    throw new NotImplementedException("not implemented.");
  }
}
