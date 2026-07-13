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

package com.aliyun.odps.storage.internal.models;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class BlobWriteItem {

  public enum ChecksumType {
    None(0),
    Crc32(1),
    MD5(2);

    private int id;

    ChecksumType(int id) {
      this.id = id;
    }
    public int getId() {
      return id;
    }
  }

  private static class Header {
    @SerializedName("PartitionValues")
    private List<String> partitionValues;

    @SerializedName("ColumnIndex")
    private long columnIndex;

    @SerializedName("DistributionKey")
    private String distributionKey;

    @SerializedName("ContentType")
    private String mimeType;

    Header(List<String> partitionValues, long columnIndex, String distributionKey, String mimeType) {
      this.partitionValues = partitionValues;
      this.columnIndex = columnIndex;
      this.distributionKey = distributionKey;
      this.mimeType = mimeType;
    }
  }

  private static class Footer {

    @SerializedName("Checksum")
    private Checksum checksum;

    Footer(Checksum checksum) {
      this.checksum = checksum;
    }

    static class Checksum {

      @SerializedName("Type")
      private int type;

      @SerializedName("Crc32")
      private Long crc32;

      @SerializedName("MD5")
      private String md5;

      Checksum(ChecksumType type, Long crc32, String md5) {
        this.type = type.getId();
        this.crc32 = crc32;
        this.md5 = md5;
      }
    }
  }

  private final Header header;
  private final Footer footer;
  private final byte[] data;

  private static final Gson GSON = new Gson();

  private BlobWriteItem(Header header, Footer footer, byte[] data) {
    this.header = header;
    this.footer = footer;
    this.data = data;
  }

  /**
   * Serializes this BlobWriteItem directly into the given OutputStream, avoiding
   * intermediate byte array allocation for the item itself.
   *
   * @param out The OutputStream to write the serialized data to.
   * @throws IOException If an I/O error occurs.
   */
  public void serializeTo(OutputStream out) throws IOException {
    byte[] headerBytes = GSON.toJson(this.header).getBytes(StandardCharsets.UTF_8);
    byte[] footerBytes = GSON.toJson(this.footer).getBytes(StandardCharsets.UTF_8);
    // Write Header: Length + Data
    writeLongLittleEndian(out, headerBytes.length);
    out.write(headerBytes);
    // Write Data: Length + Data
    writeLongLittleEndian(out, this.data.length);
    out.write(this.data);
    // Write Footer: Length + Data
    writeLongLittleEndian(out, footerBytes.length);
    out.write(footerBytes);
  }

  /**
   * A highly efficient utility method to serialize a list of BlobWriteItems into a single byte stream.
   * It creates a single output stream and writes each item directly into it, minimizing
   * temporary object allocation and memory copies.
   *
   * @param items The list of BlobWriteItems to serialize.
   * @return A single byte array representing all items concatenated.
   * @throws IOException If an I/O error occurs during serialization of any item.
   */
  public static byte[] writeBlobs(List<BlobWriteItem> items) throws IOException {
    if (items == null || items.isEmpty()) {
      return new byte[0];
    }

    // 预估总大小以优化 ByteArrayOutputStream 的初始容量，减少扩容
    // 假设每个 item 的元数据开销平均为 256 字节，这是一个保守的估计
    int estimatedSize = items.stream()
      .mapToInt(item -> item.data.length + 256)
      .sum();

    ByteArrayOutputStream finalStream = new ByteArrayOutputStream(estimatedSize);
    for (BlobWriteItem item : items) {
      item.serializeTo(finalStream);
    }
    return finalStream.toByteArray();
  }

  // 使用 ThreadLocal 来为每个线程提供一个可重用的 ByteBuffer，避免在热点路径上重复创建。
  private static final ThreadLocal<ByteBuffer> LE_LONG_BUFFER =
    ThreadLocal.withInitial(() -> {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      return buffer;
    });

  private static void writeLongLittleEndian(OutputStream out, long value) throws IOException {
    ByteBuffer buffer = LE_LONG_BUFFER.get();
    buffer.clear();
    buffer.putLong(value);
    out.write(buffer.array());
  }

  public static Builder builder() {
    return new Builder();
  }


  /**
   * A builder for creating immutable {@link BlobWriteItem} instances.
   */
  public static class Builder {

    private byte[] data;
    private List<String> partitionValues = new ArrayList<>();
    private long columnId;
    private String primaryKey;
    private Footer.Checksum checksum;
    private String mimeType;

    public Builder() {
      // Set default checksum to NONE.
      this.checksum = new Footer.Checksum(ChecksumType.None, null, null);
    }

    public Builder data(byte[] data) {
      this.data = Objects.requireNonNull(data, "Data cannot be null.");
      return this;
    }

    public Builder partitionValues(List<String> partitionValues) {
      this.partitionValues =
        Objects.requireNonNull(partitionValues, "PartitionValues cannot be null.");
      return this;
    }

    public Builder mimeType(String mimeType) {
      this.mimeType = mimeType;
      return this;
    }

    public Builder columnId(long columnId) {
      this.columnId = columnId;
      return this;
    }

    public Builder distributionKey(String distributionKey) {
      this.primaryKey = distributionKey;
      return this;
    }

    /**
     * Sets the checksum, automatically calculated from the provided data.
     * Note: This requires the 'data' to be set beforehand.
     *
     * @return The builder instance.
     */
    public Builder withChecksum(ChecksumType type) {
      if (this.data == null) {
        throw new IllegalStateException("Data must be set before calculating checksum.");
      }
      switch (type) {
        case Crc32:
          CRC32 crc = new CRC32();
          crc.update(this.data);
          this.checksum = new Footer.Checksum(ChecksumType.Crc32, crc.getValue(), null);
          break;
        case MD5:
          String md5Hex = DigestUtils.md5Hex(this.data);
          this.checksum = new Footer.Checksum(ChecksumType.MD5, null, md5Hex);
          break;
        case None:
        default:
          this.checksum = new Footer.Checksum(ChecksumType.None, null, null);
      }
      return this;
    }

    /**
     * Builds the final, immutable {@link BlobWriteItem} instance.
     *
     * @return A new BlobWriteItem.
     */
    public BlobWriteItem build() {
      Objects.requireNonNull(data, "Data is required.");

      Header header = new Header(this.partitionValues, this.columnId, this.primaryKey, this.mimeType);
      Footer footer = new Footer(this.checksum);

      return new BlobWriteItem(header, footer, this.data);
    }
  }
}
