package com.aliyun.odps.udf;

/**
 * Base StorageHandler class, user-defined StorageHandler shall extend from this class.
 * The class provides interfaces to reason about the Extractor/Outputer implemented by the user,
 * for converting raw byte stream into records and vice versa.
 **/
public abstract class OdpsStorageHandler{

  /**
   * Getter for underlying extractor class
   * @return Class description for the extractor class
   **/
  public abstract Class<? extends Extractor> getExtractorClass();

  /**
   * Getter for underlying outputer class
   * @return Class description for the outputer class
   **/
  public abstract Class<? extends Outputer> getOutputerClass();
}
