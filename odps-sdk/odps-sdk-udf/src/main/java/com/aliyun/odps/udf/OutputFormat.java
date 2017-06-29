package com.aliyun.odps.udf;

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.exec.Reporter;

import java.io.IOException;
import java.io.OutputStream;

/**
 *  TODO: see if we can remove this
 * OutputFormat describes the output-specification
 */
public interface OutputFormat {

  /**
   *
   * @param name the output file to write to
   * @param conf framework configuration
   * @param reporter mechanism for reporting progress while writing to output.
   * @return a {@link RecordWriter} to write the output for the job.
   * @throws IOException
   */
  RecordWriter getRecordWriter(String name,
                               Configuration conf,
                               Reporter reporter) throws IOException;
}
