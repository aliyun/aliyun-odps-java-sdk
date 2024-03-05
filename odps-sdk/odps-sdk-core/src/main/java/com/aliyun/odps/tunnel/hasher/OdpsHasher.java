package com.aliyun.odps.tunnel.hasher;

/**
 * Hasher function interface
 */
public interface OdpsHasher<T> {

  /**
   * Get hash value
   * @param val
   * @return
   */
  @SuppressWarnings("unchecked")
  int hash(T val);

  default T normalizeType(Object value) {
    return (T)value;
  }
}
