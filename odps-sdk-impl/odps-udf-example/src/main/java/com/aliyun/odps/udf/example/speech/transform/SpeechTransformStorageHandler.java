package com.aliyun.odps.udf.example.speech.transform;

import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.OdpsStorageHandler;
import com.aliyun.odps.udf.Outputer;
import com.aliyun.odps.udf.example.speech.SpeechSentenceSnrExtractor;

public class SpeechTransformStorageHandler extends OdpsStorageHandler {

  @Override
  public Class<? extends Extractor> getExtractorClass() {
    return SpeechRawDataExtractor.class;
  }

  @Override
  public Class<? extends Outputer> getOutputerClass() {
        return SpeechRawDataOutputer.class;
  }
}