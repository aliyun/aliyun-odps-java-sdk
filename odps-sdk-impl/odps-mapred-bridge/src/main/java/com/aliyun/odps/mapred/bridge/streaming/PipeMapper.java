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
import java.util.Map;

import com.aliyun.odps.utils.GsonObjectBuilder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.bridge.streaming.io.InputWriter;
import com.aliyun.odps.mapred.bridge.streaming.io.OutputReader;
import com.aliyun.odps.mapred.conf.JobConf;

/**
 * A generic Mapper bridge.
 * It delegates operations to an external program via stdin and stdout.
 */
public class PipeMapper extends PipeMapRed implements Mapper {

  // odps's table input always ignore key
  private boolean ignoreKey = true;
  private boolean skipping = false;

  private byte[] mapOutputFieldSeparator;
  private byte[] mapInputFieldSeparator;
  private int numOfMapOutputKeyFields = 1;

  @Override
  String getPipeCommand(JobConf job) {
    String str = job.get("stream.map.streamprocessor");
    if (str == null) {
      return str;
    }
    try {
      return URLDecoder.decode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      System.err.println("stream.map.streamprocessor in jobconf not found");
      return null;
    }
  }

  boolean getDoPipe() {
    return true;
  }

  @Override
  public void setup(TaskContext context) throws IOException {
    com.aliyun.odps.data.TableInfo ti = context.getInputTableInfo();
    // XXX not available for exstore case
    String input = "/" + ti.getProjectName() + "/" + ti.getTableName() + "/" + ti.getPartPath();
    context.getJobConf().set("map.input.file", input);
    context.getJobConf().set("map.input.columns", StringUtils.join(ti.getCols(), ","));

    try {
      Gson gson = GsonObjectBuilder.get();
      Map<String, Object> inputConfigs = gson.fromJson(
                      context.getJobConf().get("stream.map.input.configs", null),
                      new TypeToken<Map<String, Object>>() {}.getType());
      if (inputConfigs == null) {
        throw new RuntimeException("input configs is null");
      }

      String inputKey = ti.getProjectName() + "." + ti.getTableName();
      Map<String, String> inputConfig = (Map<String, String>) inputConfigs.get(inputKey);
      for (Map.Entry<String, String> e : inputConfig.entrySet()) {
        context.getJobConf().set(e.getKey(), e.getValue());
      }
    } catch (Exception e) {
      throw new RuntimeException("fail to apply input configs");
    }

    configure(context.getJobConf());
  }

  @Override
  public void cleanup(TaskContext context) throws IOException {
    close();
  }

  public void configure(JobConf job) {
    super.configure(job);
    //disable the auto increment of the counter. For streaming, no of 
    //processed records could be different(equal or less) than the no of 
    //records input.
    //SkipBadRecords.setAutoIncrMapperProcCount(job, false);
    //skipping = job.getBoolean(MRJobConfig.SKIP_RECORDS, false);
    //if (mapInputWriterClass_.getCanonicalName().equals(TextInputWriter.class.getCanonicalName())) {
    //  String inputFormatClassName = job.getClass("mapred.input.format.class", TextInputFormat.class).getCanonicalName();
    //  ignoreKey = job.getBoolean("stream.map.input.ignoreKey", 
    //    inputFormatClassName.equals(TextInputFormat.class.getCanonicalName()));
    //}

    try {
      mapOutputFieldSeparator =
          unescapeSeparator(job.get("stream.map.output.field.separator", "\t")).getBytes("UTF-8");
      mapInputFieldSeparator =
          unescapeSeparator(job.get("stream.map.input.field.separator", "\t")).getBytes("UTF-8");
      numOfMapOutputKeyFields = job.getInt("stream.num.map.output.key.fields", 1);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("The current system does not support UTF-8 encoding!", e);
    }
  }

  // Do NOT declare default constructor
  // (MapRed creates it reflectively)

  @Override
  public void map(long keyRaw, Record valueRaw, TaskContext context) throws IOException {
    if (outThread_ == null) {
      startOutputThreads(context);
    }
    if (outerrThreadsThrowable != null) {
      mapRedFinished();
      throw new IOException("MROutput/MRErrThread failed:",
                            outerrThreadsThrowable);
    }
    try {
      // 1/4 Hadoop in
      numRecRead_++;
      maybeLogRecord();

      // 2/4 Hadoop to Tool
      if (numExceptions_ == 0) {
        //if (!this.ignoreKey) {
        //  inWriter_.writeKey(key);
        //}
        inWriter_.writeValue(valueRaw);
        if (skipping) {
          //flush the streams on every record input if running in skip mode
          //so that we don't buffer other records surrounding a bad record. 
          clientOut_.flush();
        }
      } else {
        numRecSkipped_++;
      }
    } catch (IOException io) {
      numExceptions_++;
      if (numExceptions_ > 1 || numRecWritten_ < minRecWrittenToEnableSkip_) {
        // terminate with failure
        LOG.info(getContext(), io);
        mapRedFinished();
        throw io;
      } else {
        // terminate with success:
        // swallow input records although the stream processor failed/closed
      }
    }
  }

  public void close() {
    mapRedFinished();
  }

  @Override
  public byte[] getInputSeparator() {
    return mapInputFieldSeparator;
  }

  @Override
  public byte[] getFieldSeparator() {
    return mapOutputFieldSeparator;
  }

  @Override
  public int getNumOfKeyFields() {
    return numOfMapOutputKeyFields;
  }

  @Override
  InputWriter createInputWriter() throws IOException {
    return super.createInputWriter(mapInputWriterClass_);
  }

  @Override
  OutputReader createOutputReader() throws IOException {
    return super.createOutputReader(mapOutputReaderClass_);
  }

}
