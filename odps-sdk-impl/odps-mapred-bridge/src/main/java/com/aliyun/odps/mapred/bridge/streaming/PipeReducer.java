/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mapred.bridge.streaming;

import static com.aliyun.odps.mapred.utils.UTF8ByteArrayUtils.unescapeSeparator;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.bridge.streaming.io.InputWriter;
import com.aliyun.odps.mapred.bridge.streaming.io.OutputReader;
import com.aliyun.odps.mapred.conf.JobConf;

/**
 * A generic Reducer bridge.
 * It delegates operations to an external program via stdin and stdout.
 */
public class PipeReducer extends PipeMapRed implements Reducer {

  private byte[] reduceOutFieldSeparator;
  private byte[] reduceInputFieldSeparator;
  private int numOfReduceOutputKeyFields = 1;
  private boolean skipping = false;

  @Override
  String getPipeCommand(JobConf job) {
    String str = job.get("stream.reduce.streamprocessor");
    if (str == null) {
      return str;
    }
    try {
      return URLDecoder.decode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      System.err.println("stream.reduce.streamprocessor in jobconf not found");
      return null;
    }
  }

  boolean getDoPipe() {
    String argv = getPipeCommand(job_);
    // Currently: null is identity reduce. REDUCE_NONE is no-map-outputs.
    // FIXME
    return (argv != null) && !"NONE".equals(argv);
  }

  public void configure(JobConf job) {
    super.configure(job);
    //disable the auto increment of the counter. For streaming, no of 
    //processed records could be different(equal or less) than the no of 
    //records input.
    //SkipBadRecords.setAutoIncrReducerProcCount(job, false);
    //skipping = job.getBoolean(MRJobConfig.SKIP_RECORDS, false);

    try {
      reduceOutFieldSeparator =
          unescapeSeparator(job_.get("stream.reduce.output.field.separator", "\t"))
              .getBytes("UTF-8");
      reduceInputFieldSeparator =
          unescapeSeparator(job_.get("stream.reduce.input.field.separator", "\t"))
              .getBytes("UTF-8");
      this.numOfReduceOutputKeyFields = job_.getInt("stream.num.reduce.output.key.fields", 1);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("The current system does not support UTF-8 encoding!", e);
    }
  }

  @Override
  public void setup(TaskContext context) throws IOException {
    configure(context.getJobConf());
  }

  @Override
  public void cleanup(TaskContext context) throws IOException {
    close();
  }

  @Override
  public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {

    // init
    if (doPipe_ && outThread_ == null) {
      startOutputThreads(context);
    }
    try {
      while (values.hasNext()) {
        Record val = values.next();
        numRecRead_++;
        maybeLogRecord();
        if (doPipe_) {
          if (outerrThreadsThrowable != null) {
            mapRedFinished();
            throw new IOException("MROutput/MRErrThread failed:",
                                  outerrThreadsThrowable);
          }
          // FIXME shuffle key-value both just single Text field now.
          inWriter_.writeKey(key.get(0));
          inWriter_.writeValue(val.get(0));
        } else {
          // "identity reduce"
          // FIXME field split & type convert
          Record record = context.createOutputRecord();
          record.setString(0, key.getString(0));
          record.setString(1, val.getString(0));
          context.write(record);
        }
      }
      if (doPipe_ && skipping) {
        //flush the streams on every record input if running in skip mode
        //so that we don't buffer other records surrounding a bad record. 
        clientOut_.flush();
      }
    } catch (IOException io) {
      // a common reason to get here is failure of the subprocess.
      // Document that fact, if possible.
      String extraInfo = "";
      try {
        int exitVal = sim.exitValue();
        if (exitVal == 0) {
          extraInfo = "subprocess exited successfully\n";
        } else {
          extraInfo = "subprocess exited with error code " + exitVal + "\n";
        }
        ;
      } catch (IllegalThreadStateException e) {
        // hmm, but child is still running.  go figure.
        extraInfo = "subprocess still running\n";
      }
      ;
      mapRedFinished();
      throw new IOException(extraInfo + getContext() + io.getMessage());
    }
  }

  public void close() {
    mapRedFinished();
  }

  @Override
  public byte[] getInputSeparator() {
    return reduceInputFieldSeparator;
  }

  @Override
  public byte[] getFieldSeparator() {
    return reduceOutFieldSeparator;
  }

  @Override
  public int getNumOfKeyFields() {
    return numOfReduceOutputKeyFields;
  }

  @Override
  InputWriter createInputWriter() throws IOException {
    return super.createInputWriter(reduceInputWriterClass_);
  }

  @Override
  OutputReader createOutputReader() throws IOException {
    return super.createOutputReader(reduceOutputReaderClass_);
  }

}
