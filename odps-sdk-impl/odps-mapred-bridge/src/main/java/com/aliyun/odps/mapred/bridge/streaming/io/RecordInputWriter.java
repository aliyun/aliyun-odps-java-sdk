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

import java.io.DataOutput;
import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.bridge.streaming.PipeMapRed;

/**
 * InputWriter that writes the client's input as text.
 */
public class RecordInputWriter extends InputWriter<Object, Record> {

  private DataOutput clientOut;
  private byte[] inputSeparator;

  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    clientOut = pipeMapRed.getClientOutput();
    // FIXME  key-value separator VS field separator
    inputSeparator = pipeMapRed.getInputSeparator();
  }

  @Override
  public void writeKey(Object key) throws IOException {
    // no key for table record
  }

  @Override
  public void writeValue(Record value) throws IOException {
    for (int col = 0; col < value.getColumnCount(); col++) {
      if (col > 0) {
        clientOut.write(inputSeparator);
      }
      Object colValue = value.get(col);
      if (colValue == null) {
        colValue = "";
      }
      TextInputWriter.writeUTF8(colValue, clientOut);
    }
    clientOut.write('\n');
  }

}
