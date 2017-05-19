package com.aliyun.odps.udf;

/**
 *  Hive-compatible APIs for building storage handler
 *  TODO: see if we can remove bridged version of inputformat and outputformat, then we can
 *  have BridgeHiveStorageHandler implements *both* BaseStorageHandler and Hive's HiveStorageHandler interfaces
 */
public interface BridgeStorageHandler {
  /**
   * Hive compatibility API
   * @return  Class providing an implementation of InputFormat
   */
  public abstract Class<? extends InputFormat> getInputFormatClass();

  /**
   * Hive compatibility API
   * @return  Class providing an implementation of OutputFormat
   */
  public abstract Class<? extends OutputFormat> getOutputFormatClass();

  /**
   * Hive compatibility API
   * @return  Class providing an implementation of RecordSerDe
   */
  public abstract Class<? extends RecordSerDe> getSerDeClass();

}
