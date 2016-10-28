package com.aliyun.odps.udf;

import com.aliyun.odps.NotImplementedException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.exec.InputSplit;
import com.aliyun.odps.io.InputStreamSet;

import java.io.IOException;

/**
 * Base extractor class, user-defined extractors shall extend from this class
 **/
public abstract class Extractor {

  /**
   * Currently the method shall be overridden only when using Hive-compatible interfaces, i.e., when
   * inputFormat/Serde are used to describe the data extraction logic.
   * TODO: use this to describe general input split.
   * @param split: input files described by InputSplit
   * @return converted InputStreamSet
   */
  public InputStreamSet splitToInputStreamSet(InputSplit split) {
    throw new NotImplementedException("No default splitToInputStreamSet method implemented.");
  }

  /**
   * Interface for setting up the extractor, implementation can be a no-op
   * @param ctx: the ExecutionContext which contains context information that may be useful
   *             for setting up user code execution environment
   * @param inputs: set of input streams, each corresponding to one input file
   * @param attributes: encapsulate any attributes needed that describe the associated input data
   **/
  public abstract void setup(ExecutionContext ctx, InputStreamSet inputs, DataAttributes attributes);

  /**
   * Interface for extracting a schematized record from an input stream
   * @return the extracted record, returning null indicates no more record is to be extracted
   **/
  public abstract Record extract() throws IOException;

  /**
   * Interface for operations upon extractor exit, implementation can be no-op
   **/
  public abstract void close();
}
