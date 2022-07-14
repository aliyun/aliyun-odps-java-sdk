package com.aliyun.odps.tunnel.hasher;

/**
 * Hash factory
 */
public interface HasherFactory {
  /**
   * get hash by type Name
   * @param typeName
   * @return
   */
  OdpsHasher getHasher(String typeName);
}
