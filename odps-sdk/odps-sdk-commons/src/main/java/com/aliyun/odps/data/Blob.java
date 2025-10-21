package com.aliyun.odps.data;

import java.io.Serializable;


import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

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

    private final byte[] referenceBytes;

    /**
     * Constructs a Blob from raw bytes (defensive copy is made).
     *
     * @param referenceBytes the reference bytes (must not be null)
     * @throws IllegalArgumentException if referenceBytes is null
     */
    public Blob(byte[] referenceBytes) {
        if (referenceBytes == null) {
            throw new IllegalArgumentException("Blob cannot be created from null bytes");
        }
        this.referenceBytes = referenceBytes.clone();
    }

    /**
     * Gets a defensive copy of the reference bytes.
     *
     * @return a new byte array containing the reference
     */
    public byte[] getReferenceBytes() {
        return referenceBytes.clone();
    }

    /**
     * Converts the reference to a Base64-encoded string (for human-readable logging, serialization, or storage).
     *
     * @return Base64 string representation (e.g., "aGVsbG8=")
     */
    public String toBase64() {
        return Base64.getEncoder().encodeToString(referenceBytes);
    }

    /**
     * Creates a Blob from a Base64-encoded string.
     *
     * @param base64String the Base64 string (must not be null or invalid)
     * @return a new Blob instance
     * @throws IllegalArgumentException if base64String is invalid
     */
    public static Blob fromBase64(String base64String) {
        Objects.requireNonNull(base64String, "Base64 string cannot be null");
        try {
            byte[] bytes = Base64.getDecoder().decode(base64String);
            return new Blob(bytes);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Base64 string for Blob", e);
        }
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
        return Arrays.equals(referenceBytes, that.referenceBytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(referenceBytes);
    }

    @Override
    public String toString() {
        // Use Base64 for readable output (truncate if too long)
        String base64 = toBase64();
        return "Blob{" +
               "reference=" + (base64.length() > 20 ? base64.substring(0, 17) + "..." : base64) +
               '}';
    }
}
