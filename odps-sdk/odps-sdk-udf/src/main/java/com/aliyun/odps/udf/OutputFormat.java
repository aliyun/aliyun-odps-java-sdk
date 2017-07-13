package com.aliyun.odps.udf;

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.exec.Reporter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

/**
 *  TODO: see if we can remove this
 * OutputFormat describes the output-specification
 */
public interface OutputFormat {

  /**
   * @param conf framework configuration
   * @param name the output file to write to
   * @param valueClass value class, should be Class<? extends org.apache.hadoop.io.Writable>
   * @param isCompressed whether compress outputing file
   * @param tableProperties table properties
   * @param reporter mechanism for reporting progress while writing to output.
   * @return a {@link RecordWriter} to write the output for the job.
   * @throws IOException
   */
  RecordWriter getRecordWriter(Configuration conf,
                               String name,
                               final Class valueClass,
                               boolean isCompressed,
                               Properties tableProperties,
                               Reporter reporter) throws IOException;
}
