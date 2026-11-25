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

package com.aliyun.odps.table.record.accessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.aliyun.odps.data.Vector;
import com.aliyun.odps.exceptions.SchemaMismatchRuntimeException;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.junit.Test;

public class ArrowVectorRecordConversionTest {

  @Test
  public void typedVectorChecksDimensionAndPreservesNulls() throws Exception {
    try (RootAllocator allocator = new RootAllocator(1 << 20);
         ListVector list = ListVector.empty("embedding", allocator);
         UnionListWriter writer = list.getWriter()) {
      writer.allocate();
      writer.setPosition(0);
      writer.startList();
      writer.float8().writeFloat8(1.25);
      writer.float8().writeFloat8(-2.5);
      writer.endList();
      writer.setPosition(1);
      writer.writeNull();
      writer.setPosition(2);
      writer.startList();
      writer.float8().writeFloat8(3.0);
      writer.endList();
      writer.setValueCount(3);

      TypeInfo type = TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.DOUBLE, 2);
      ArrowVectorAccessor accessor = ArrowToRecordConverter.createColumnVectorAccessor(list, type);
      Vector valid = (Vector) ArrowToRecordConverter.getData(accessor, type, 0);
      assertEquals(2, valid.dimension());
      assertEquals(1.25, valid.getElement(0).doubleValue(), 0.0);
      assertEquals(-2.5, valid.getElement(1).doubleValue(), 0.0);
      assertNull(ArrowToRecordConverter.getData(accessor, type, 1));
      assertThrows(SchemaMismatchRuntimeException.class,
          () -> ArrowToRecordConverter.getData(accessor, type, 2));
    }
  }

  @Test
  public void bareVectorRetainsFloatValuesAndRowDimension() throws Exception {
    try (RootAllocator allocator = new RootAllocator(1 << 20);
         ListVector list = ListVector.empty("embedding", allocator);
         UnionListWriter writer = list.getWriter()) {
      writer.allocate();
      writer.setPosition(0);
      writer.startList();
      writer.float4().writeFloat4(3.5f);
      writer.endList();
      writer.setValueCount(1);

      TypeInfo type = TypeInfoFactory.VECTOR;
      ArrowVectorAccessor accessor = ArrowToRecordConverter.createColumnVectorAccessor(list, type);
      Vector value = (Vector) ArrowToRecordConverter.getData(accessor, type, 0);
      assertEquals(1, value.dimension());
      assertEquals(3.5f, value.getElement(0).floatValue(), 0.0f);
    }
  }
}
