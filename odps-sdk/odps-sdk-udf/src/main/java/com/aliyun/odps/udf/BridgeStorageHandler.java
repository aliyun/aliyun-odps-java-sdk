package com.aliyun.odps.udf;

/**
 *  Hive-compatible APIs for building storage handler
 *  TODO: see if we can remove bridged version of inputformat and outputformat, then we can
 *  have BridgeHiveStorageHandler implements *both* BaseStorageHandler and Hive's HiveStorageHandler interfaces
 */
public interface BridgeStorageHandler extends BaseStorageHandler {

  @Override
  public abstract Class<? extends Extractor> getExtractorClass();

  @Override
  public abstract Class<? extends Outputer> getOutputerClass();

  /**
   * Hive compatibility API
   * @return  Class providing an implementation of InputFormat
   */
  public abstract Class getInputFormatClass();

  /**
   * Hive compatibility API
   * @return  Class providing an implementation of OutputFormat
   */
  public abstract Class getOutputFormatClass();

  /**
   * Hive compatibility API
   * @return  Class providing an implementation of AbstractSerDe
   */
  public abstract Class getSerDeClass();

}
