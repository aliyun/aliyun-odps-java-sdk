package com.aliyun.odps.udf;

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.exec.InputSplit;
import com.aliyun.odps.exec.Reporter;

import java.io.IOException;

/**
 * TODO: see if we can remove this
 */
public interface InputFormat {

  /**
   * Logically split the set of input files for the job.
   *
   * Each InputSplit is then assigned to an individual mapper
   * for processing.
   *
   * Note: The split is a logical split of the inputs and the
   * input files are not physically split into chunks. For e.g. a split could
   * be input-file-path, start, offset tuple.
   *
   * @param numSplits the desired number of splits, a hint.
   * @return an array of {@link InputSplit}s for the job.
   */
  InputSplit[] getSplits(Configuration conf,
                         int numSplits) throws IOException;

  /**
   * Get the RecordReader for the given InputSplit.
   *
   * It is the responsibility of the RecordReader to respect
   * record boundaries while processing the logical split to present a
   * record-oriented view to the individual task.
   */
  RecordReader getRecordReader(InputSplit split,
                               Configuration conf,
                               Reporter reporter) throws IOException;
}
