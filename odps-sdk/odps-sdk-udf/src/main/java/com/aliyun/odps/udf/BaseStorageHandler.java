package com.aliyun.odps.udf;

/**
 * This is the interface on top of which both Hive-compatible and ODPS storage handlers are built
 * Internal usage *ONLY* within ODPS framework. User storage handler should instead extends either
 * a) {@link OdpsStorageHandler}: to implement ODPS style extract/output interface, or
 * b) {@link BridgeStorageHandler}: to build Hive-compatible Serde/Formatter modules
 */
public interface BaseStorageHandler {
  /**
   * Getter for the underlying {@link Extractor} in {@link OdpsStorageHandler}
   * @return Class description for the extractor class
   **/
   Class<? extends Extractor> getExtractorClass();

  /**
   * Getter for the underlying {@link Outputer} in {@link OdpsStorageHandler}
   * @return Class description for the outputer class
   **/
   Class<? extends Outputer> getOutputerClass();
}

