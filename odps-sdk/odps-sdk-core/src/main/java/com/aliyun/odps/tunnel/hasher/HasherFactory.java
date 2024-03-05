package com.aliyun.odps.tunnel.hasher;

import com.aliyun.odps.OdpsType;

/**
 * Hash factory
 */
interface HasherFactory {
  /**
   * get hash by type
   * @param type
   * @return haser
   */
  OdpsHasher getHasher(OdpsType type);
}
