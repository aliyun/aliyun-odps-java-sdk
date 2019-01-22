package com.aliyun.odps.udf;

/**
 * Provides interface to access outputer-only related attributes.
 */
public abstract class TableOutputerAttributes extends DataAttributes {

  /**
   * Whether the underlying output operation is insert overwrite.
   * @return true: insert overwrite;
   *         false: insert into
   */
  public abstract boolean getIsOverwrite();

  /**
   * @return true if the operation is dynamic partition otherwise false
   */
  public abstract boolean isDynamicPartition();

  /**
   * @return The indices of dynamic partition columns in output record or null if the operation is not dynamic partition
   */
  public abstract int[] getDynamicPartitionColumnIndices();

  /**
   * @return Static partition name, or
   *         return static partition prefix (empty if there is no static prefix) for dynamic partitioned output table, or
   *         return null if output table is not partitioned
   */
  public abstract String getStaticPartitionName();

}
