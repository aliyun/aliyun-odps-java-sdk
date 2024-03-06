/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.table.arrow.accessor;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;

/**
 * Arrow column vector accessor for decimal.
 */
public class ArrowDecimalExtensionAccessor extends ArrowVectorAccessor {

  protected final FixedSizeBinaryVector decimalVector;

  public ArrowDecimalExtensionAccessor(FixedSizeBinaryVector decimalVector) {
    super(decimalVector);
    this.decimalVector = decimalVector;
  }

  /**
   *  legacy decimal memory layout check runtime_decimal.h
   *     int8_t  mNull;
   *     int8_t  mSign;
   *     int8_t  mIntg;
   *     int8_t  mFrac; only 0, 1, 2
   *     int32_t mData[6];
   *     int8_t mPadding[4]; //For Memory Align
   */
  public BigDecimal getDecimal(int rowId) {
    byte[] val = decimalVector.get(rowId);
    ByteBuffer buffer = ByteBuffer.wrap(val);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    byte mSign = buffer.get(1);
    byte mIntg = buffer.get(2);
    byte mFrac = buffer.get(3);
    StringBuilder decimalBuilder = new StringBuilder();
    if (mSign > 0) {
      decimalBuilder.append("-");
    }
    for (int j = mIntg; j > 0; j--) {
      int num = buffer.getInt(8 + j * 4);
      if (j == mIntg) {
        decimalBuilder.append(num);
      } else {
        decimalBuilder.append(String.format("%09d", num));
      }
    }
    decimalBuilder.append(".");
    for(int j = 0; j < mFrac; j++) {
      int num = buffer.getInt(8 - 4 * j);
      decimalBuilder.append(String.format("%09d", num));
    }
    BigDecimal bd = new BigDecimal(decimalBuilder.toString());
    return bd;
  }
}