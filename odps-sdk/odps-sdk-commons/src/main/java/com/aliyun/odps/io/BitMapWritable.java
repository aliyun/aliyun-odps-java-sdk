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

package com.aliyun.odps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BitMapWritable implements Writable {
  
  private byte[] bits;
  private int size;
  
  public BitMapWritable(int size) {
    if (size < 0) {
      throw new IllegalArgumentException();
    }
    int arraySize = size / 8;
    if (size % 8 != 0) {
      arraySize ++;
    }
    bits = new byte[arraySize];
    this.size = arraySize;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(bits);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    in.readFully(bits);
  }
  
  public boolean get(int index) {
    int i = index / 8;
    int j = index % 8;
    byte flag = bits[i];
    int ret = (flag >>> j) & 0x01;
    return ret != 0;
  }
  
  public void set(int index, boolean flag) {
    int i = index / 8;
    int j = index % 8;
    int mask = flag ? 0x01 : 0;
    mask = mask << j;
    bits[i] = (byte) (bits[i] | mask);
  }
  
  public int getDataSize() {
    return size;
  }

}