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

package com.aliyun.odps.sqa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader; /**
 * class: SessionRecordSetIterator It is used in getSessionResultSetByInstanceTunnel
 */
public class SessionRecordSetIterator implements Iterator<Record> {

  private List<Record> buffer;
  private static final long FETCH_SIZE = 10000L;
  private long cursor = 0;
  private int idx = 0;
  private long fetchSize = 0;
  private long recordCount;
  private long offset;
  private long sizeLimit;
  private long currentReadSize = 0;
  private InstanceTunnel.DownloadSession session;
  private TunnelRecordReader reader;

  public SessionRecordSetIterator(InstanceTunnel.DownloadSession session
      , TunnelRecordReader reader
      , long recordCount
      , long offset
      , long sizeLimit) {
    this.session = session;
    this.reader = reader;
    this.recordCount = recordCount;
    this.offset = offset;
    this.sizeLimit = sizeLimit;
  }

  public InstanceTunnel.DownloadSession getSession() {
    return session;
  }

  public TunnelRecordReader getReader() {
    return reader;
  }

  @Override
  public boolean hasNext() {
    return cursor < recordCount;
  }

  @Override
  public Record next() {
    if (buffer == null || idx == buffer.size()) {
      fillBuffer();
    }
    cursor++;
    return buffer.get(idx++);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  private void fillBuffer() {
    idx = 0;
    // use original reader when fill buffer at first time if record count less than 10000
    if (cursor == 0 && recordCount <= FETCH_SIZE) {
      // reuse reader
    } else {
      reader = openNewReader();
    }
    buffer = new ArrayList<Record>();
    Record r = null;
    try {
      while ((r = reader.read()) != null) {
        buffer.add(r);
        if (sizeLimit > 0) {
          if (currentReadSize + reader.getTotalBytes() > sizeLimit) {
            throw new RuntimeException("InvalidArgument: sizeLimit, fetched data is larger than limit size");
          }
        }
      }
      currentReadSize += reader.getTotalBytes();
    } catch (IOException e) {
      throw new RuntimeException("Read from reader failed:", e);
    }
  }

  private TunnelRecordReader openNewReader() {
    fetchSize = recordCount - cursor <= FETCH_SIZE ? recordCount - cursor : FETCH_SIZE;
    try {
      return session.openRecordReader(cursor + offset, fetchSize);
    } catch (TunnelException e) {
      throw new RuntimeException("Open reader failed: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new RuntimeException("Open reader failed: " + e.getMessage(), e);
    }
  }
}
