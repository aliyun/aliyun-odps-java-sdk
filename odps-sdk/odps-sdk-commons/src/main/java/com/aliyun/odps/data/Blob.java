package com.aliyun.odps.data;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Immutable entity class representing a Blob reference in the storage service.
 * This class is designed for use in table entities (e.g., as a column type),
 * following the two-phase commit pattern:
 * <ul>
 *   <li>After uploading data via SQL or MaxStorage, a Blob is obtained.</li>
 *   <li>This reference is stored in a table column (e.g., in a {@code TableEntity} subclass).</li>
 *   <li>During download, the reference is retrieved from the table and passed to {@link BlobManager#downloadBlob(Blob)}.</li>
 * </ul>
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Blob implements Serializable {

    private final String blobReference;  // 已就绪的引用, BASE64 格式

    private transient final InputStream rawStream; // 原始流

    private transient final byte[] rawBytes; // 原始字节（批量上传模式使用）

    private final String mimeType; // Blob 的 MIME 类型（可选）

    private final String customFileName; // Blob 的自定义文件名（可选）

    private transient Function<Void, Blob> uploadTask; // 懒加载的上传任务

    /**
     * Constructs a Blob from raw bytes (defensive copy is made).
     *
     * @param blobReference the reference bytes
     * @throws IllegalArgumentException if referenceBytes is null
     */
    private Blob(InputStream rawStream, byte[] rawBytes, String blobReference,
                 Function<Void, Blob> uploadTask, String mimeType, String customFileName) {
        this.rawStream = rawStream;
        this.rawBytes = rawBytes;
        this.blobReference = blobReference;
        this.uploadTask = uploadTask;
        this.mimeType = mimeType;
        this.customFileName = customFileName;
    }

    public static Blob fromInputStream(InputStream stream) {
        // 创建一个最原始的、只包含流的Blob
        return new Blob(stream, null, null, null, null, null);
    }

    public static Blob fromInputStream(InputStream stream, String mimeType) {
        return new Blob(stream, null, null, null, mimeType, null);
    }

    public static Blob fromInputStream(InputStream stream, String mimeType, String customFileName) {
        return new Blob(stream, null, null, null, mimeType, customFileName);
    }

    public static Blob fromBytes(byte[] data) {
        return new Blob(null, data, null, null, null, null);
    }

    public static Blob fromBytes(byte[] data, String mimeType) {
        return new Blob(null, data, null, null, mimeType, null);
    }

    public static Blob fromBytes(byte[] data, String mimeType, String customFileName) {
        return new Blob(null, data, null, null, mimeType, customFileName);
    }

    public static Blob fromReference(String blobReference) {
        return new Blob(null, null, blobReference, null, null, null);
    }

    public boolean isRawStream() {
        return this.rawStream != null;
    }

    public InputStream getRawStream() {
        return rawStream;
    }

    public boolean isRawBytes() {
        return this.rawBytes != null;
    }

    public byte[] getRawBytes() {
        return rawBytes;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getCustomFileName() {
        return customFileName;
    }

    public boolean isPending() {
        return this.uploadTask != null;
    }

    public Blob withUploader(BiFunction<InputStream, Long, Blob> uploader, Long columnId) {
        if (!isRawStream()) {
            throw new IllegalStateException("Cannot upload null blob.");
        }
        Function<Void, Blob> task = (ignored) -> uploader.apply(this.rawStream, columnId);
        return new Blob(null, null, null, task, this.mimeType, this.customFileName);
    }

    public String getReferenceAndUploadIfNecessary() {
        if (isPending()) {
            return this.uploadTask.apply(null).getReference();
        } else if (blobReference != null) {
            return blobReference;
        } else {
            throw new IllegalStateException("Blob was not properly prepared for upload. Ensure it is set on a Record created by a RecordWriter.");
        }
    }

    /**
     * Gets a defensive copy of the reference bytes.
     *
     * @return a new byte array containing the reference
     */
    public byte[] getReferenceBytes() {
        return Base64.getDecoder().decode(blobReference);
    }

    public String getReference() {
        return blobReference;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Blob that = (Blob) o;
        return blobReference.equals(that.blobReference);
    }

    @Override
    public int hashCode() {
        return blobReference.hashCode();
    }

    @Override
    public String toString() {
        // Use Base64 for readable output (truncate if too long)
        String ref = blobReference == null ? "null" : blobReference;
        return "Blob{" +
               "reference=" + (ref.length() > 20 ? ref.substring(0, 17) + "..." : ref) +
               '}';
    }
}
