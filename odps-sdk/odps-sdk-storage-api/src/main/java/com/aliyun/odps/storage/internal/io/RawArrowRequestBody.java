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

package com.aliyun.odps.storage.internal.io;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class RawArrowRequestBody extends RequestBody {

  List<byte[]> batches;

  Schema arrowSchema;

  IpcOption ipcOption;

  private long totalBytes = 0;

  public RawArrowRequestBody(List<byte[]> batches, Schema arrowSchema, IpcOption ipcOption) {
    this.batches = batches;
    this.arrowSchema = arrowSchema;
    this.ipcOption = ipcOption;

    for (byte[] batch : batches) {
      totalBytes += batch.length;
    }
  }

  @Override
  public MediaType contentType() {
    return MediaType.parse("application/vnd.apache.arrow.stream");
  }

  @Override
  public void writeTo(@NotNull BufferedSink sink) throws IOException {
    try (WriteChannel channel = new WriteChannel(Channels.newChannel(sink.outputStream()))) {
      MessageSerializer.serialize(channel, arrowSchema, ipcOption);
      for (byte[] batchBytes : batches) {
        sink.write(batchBytes);
      }
      if (!ipcOption.write_legacy_ipc_format) {
        channel.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
      }
      channel.writeIntLittleEndian(0);
    }
  }

  public long getTotalBytes() {
    return totalBytes;
  }
}
