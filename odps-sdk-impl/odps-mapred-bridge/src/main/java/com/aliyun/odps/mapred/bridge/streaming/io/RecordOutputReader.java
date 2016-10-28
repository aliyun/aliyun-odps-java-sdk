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

package com.aliyun.odps.mapred.bridge.streaming.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.bridge.streaming.PipeMapRed;
import com.aliyun.odps.mapred.utils.UTF8ByteArrayUtils;

//import com.aliyun.odps.util.StringUtils;

/**
 * OutputReader that reads the client's output as text.
 */
public class RecordOutputReader extends OutputReader<Object, Text[]> {

  private LineReader lineReader;
  private byte[] bytes;
  private DataInput clientIn;
  private Configuration conf;
  private int numFields;
  private byte[] separator;
  private Text line;
  private Text[] value;

  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    clientIn = pipeMapRed.getClientInput();
    conf = pipeMapRed.getConfiguration();
    numFields = pipeMapRed.getNumOfOutputFields();
    separator = pipeMapRed.getFieldSeparator();
    lineReader = new LineReader((InputStream) clientIn, conf);
    line = new Text();
    value = new Text[numFields];
  }

  @Override
  public boolean readKeyValue() throws IOException {
    if (lineReader.readLine(line) <= 0) {
      return false;
    }
    bytes = line.getBytes();
    try {
      splitFields(bytes, line.getLength(), value);
    } catch (IOException e) {
      System.err.println("Stop on line:'" + line + "'");
      throw e;
    }
    line.clear();
    return true;
  }

  @Override
  public Object getCurrentKey() throws IOException {
    // no key for record output
    return null;
  }

  @Override
  public Text[] getCurrentValue() throws IOException {
    return value;
  }

  @Override
  public String getLastOutput() {
    if (bytes != null) {
      try {
        return new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        return "<undecodable>";
      }
    } else {
      return null;
    }
  }

  // split a UTF-8 line into key and value
  private void splitFields(byte[] line, int length, Text[] fields)
      throws IOException {
    // FIXME what if reduce output line has different num of fields than output table?
    int start = 0;
    int pos = UTF8ByteArrayUtils.findBytes(line, start, length, separator);
    int k;
    for (k = 0; k < numFields - 1 && pos != -1; k++) {
      Text field = new Text();
      field.set(line, start, pos - start);
      fields[k] = field;
      start = pos + separator.length;
      pos = UTF8ByteArrayUtils.findBytes(line, start, length, separator);
    }
    if (pos == -1 && k == numFields - 1) {
      // normal
      Text field = new Text();
      field.set(line, start, length - start);
      fields[k] = field;
    } else if (k == numFields - 1) {
      // more fields
      throw new IOException("streaming output line has more fields than output schema");
    } else {
      // less fields
      throw new IOException("streaming output line has less fields than output schema");
    }
  }

}
