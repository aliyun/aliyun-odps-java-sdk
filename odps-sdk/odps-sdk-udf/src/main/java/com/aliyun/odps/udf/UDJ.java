package com.aliyun.odps.udf;

import com.aliyun.odps.Yieldable;
import com.aliyun.odps.data.Record;

import java.util.Iterator;

/**
 * UDJ (User Defined Join)
 * <p>
 * User Defined Join need to inherit this class to implement a UDJ, override join method
 * </p>
 */
public abstract class UDJ {

  /**
   * Interface for setting up the udj, implementation can be a no-op
   * @param ctx: the ExecutionContext which contains context information that may be useful
   *             for setting up user code execution environment
   * @param attributes: encapsulate any attributes needed for join operation, such as parameters described by k-v pairs
   **/
  public abstract void setup(ExecutionContext ctx, DataAttributes attributes);

  /**
   * Interface for setting up the udj, implementation can be a no-op
   * @param left: left input records, if map join hint is used (on smaller table), left input will correspond to the
   *            table on which the map join hint is applied: i.e., left input corresponds to smaller table in that case
   * @param right: right input records, in the case of map join, right input would provide the streaming input from
   *             the larger table that likely would not fit into memory as a whole
   * @param output: interaface for producing output from the joining operation
   **/
  public abstract void join(Iterator<Record> left, Iterator<Record> right, Yieldable<Record> output);

  /**
   * Interface for operations upon udj exit, implementation can be no-op
   **/
  public abstract void close();
}
