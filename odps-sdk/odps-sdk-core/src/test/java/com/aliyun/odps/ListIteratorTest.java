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

package com.aliyun.odps;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public class ListIteratorTest {

  public Iterator<String> getList() {
    return new ListIterator<String>() {
      private int step = 0;

      @Override
      protected List<String> list() {
        ++step;
        if (step == 1) {
          return new ArrayList<String>();
        } else if (step == 2) {
          ArrayList<String> list = new ArrayList<String>();
          list.add("not empty");
          return list;
        } else {
          return null;
        }
      }
    };
  }

  @Test
  public void testList() throws OdpsException {
    Iterator<String> list = getList();
    assertTrue(list.hasNext());
  }

  @Test
  public void testNextFirst() throws OdpsException {
    Iterator<String> list = getList();
    assertTrue("not empty".equals(list.next()));
  }
}
