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

package com.aliyun.odps.mapred.bridge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Queue;

import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.bridge.type.ColumnBasedRecordComparator;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class GroupingRecordIteratorTest {

  @Test
  public void testGrouping() {
    Queue<Object[]> queue = new LinkedList<Object[]>();
    queue.add(new Object[]{new Text("foo"), new LongWritable(1L)});
    queue.add(new Object[]{new Text("foo"), new LongWritable(2L)});
    queue.add(new Object[]{new Text("bar"), new LongWritable(3L)});
    queue.add(new Object[]{new Text("bar"), new LongWritable(4L)});
    Record key = new WritableRecord(SchemaUtils.fromString("k:string"));
    Record value = new WritableRecord(SchemaUtils.fromString("v:bigint"));
    Comparator<Object[]> comparator = new ColumnBasedRecordComparator(new int[]{0},
                                                                      key.getColumns());
    GroupingRecordIterator itr = new GroupingRecordIterator(queue, (WritableRecord) key,
                                                            (WritableRecord) value, comparator);
    key.set(Arrays.copyOf(queue.peek(), 1));
    Long v = 1L;
    while (itr.hasNext()) {
      Long val = (Long) itr.next().get(0);
      assertEquals(v, val);
      v++;
    }
    itr.reset();
    while (itr.hasNext()) {
      Long val = (Long) itr.next().get(0);
      assertEquals(v, val);
      v++;
    }
  }

  @Test
  public void testHasNext() {
    Queue<Object[]> queue = new LinkedList<Object[]>();
    queue.add(new Object[]{new Text("foo"), new LongWritable(1L)});
    queue.add(new Object[]{new Text("foo"), new LongWritable(2L)});
    queue.add(new Object[]{new Text("bar"), new LongWritable(3L)});
    queue.add(new Object[]{new Text("bar"), new LongWritable(4L)});
    Record key = new WritableRecord(SchemaUtils.fromString("k:string"));
    Record value = new WritableRecord(SchemaUtils.fromString("v:bigint"));
    Comparator<Object[]> comparator = new ColumnBasedRecordComparator(new int[]{0},
                                                                      key.getColumns());
    GroupingRecordIterator itr = new GroupingRecordIterator(queue, (WritableRecord) key,
                                                            (WritableRecord) value, comparator);
    key.set(Arrays.copyOf(queue.peek(), 1));
    Long v = 1L;
    while (itr.hasNext()) {
      itr.hasNext();
      Long val = (Long) itr.next().get(0);
      assertEquals(v, val);
      v++;
    }
    itr.reset();
    while (itr.hasNext()) {
      itr.hasNext();
      Long val = (Long) itr.next().get(0);
      assertEquals(v, val);
      v++;
    }
    assertEquals(5, v.longValue());
  }

  @Test
  public void testSameGroup() {
    Queue<Object[]> queue = new LinkedList<Object[]>();
    queue.add(new Object[]{new Text("foo"), new LongWritable(1L)});
    queue.add(new Object[]{new Text("foo"), new LongWritable(2L)});
    queue.add(new Object[]{new Text("foo"), new LongWritable(3L)});
    queue.add(new Object[]{new Text("foo"), new LongWritable(4L)});
    Record key = new WritableRecord(SchemaUtils.fromString("k:string"));
    Record value = new WritableRecord(SchemaUtils.fromString("v:bigint"));
    Comparator<Object[]> comparator = new ColumnBasedRecordComparator(new int[]{0},
                                                                      key.getColumns());
    GroupingRecordIterator itr = new GroupingRecordIterator(queue, (WritableRecord) key,
                                                            (WritableRecord) value, comparator);
    key.set(Arrays.copyOf(queue.peek(), 1));
    Long v = 1L;
    while (itr.hasNext()) {
      itr.hasNext();
      Long val = (Long) itr.next().get(0);
      assertEquals(v, val);
      v++;
    }
    assertEquals(5, v.longValue());
    itr.reset();
    assertFalse(itr.hasNext());
  }
}
