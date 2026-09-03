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

package com.aliyun.odps.table.read.impl.batch;

import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import com.aliyun.odps.table.read.split.InputSplitWithIndex;
import com.aliyun.odps.table.read.split.InputSplitWithRowRange;
import com.aliyun.odps.table.read.split.RowRange;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Owns one reader's isolated spill directory.
 *
 * <p>The workspace never scans, locks, or deletes another reader's directory. The caller that
 * configures the spill root is responsible for cleaning workspaces left by abnormal process
 * termination, and must do so only when no reader is using that root.
 */
final class DiskSpillWorkspace {

    private static final String NAMESPACE = "odps-sdk-disk-spill-v1";
    private static final String READER_PREFIX = "reader-";
    private static final String SPILL_PREFIX = "odps-spill-";
    private static final String SPILL_SUFFIX = ".arrow";
    private static final int MAX_SESSION_ID_LENGTH = 48;

    private final Path readerDirectory;
    private boolean deleted;

    private DiskSpillWorkspace(Path readerDirectory) {
        this.readerDirectory = readerDirectory;
    }

    static DiskSpillWorkspace create(File spillRoot) throws IOException {
        return create(spillRoot, null);
    }

    static DiskSpillWorkspace create(File spillRoot, InputSplit split) throws IOException {
        if (spillRoot == null) {
            throw new IOException("Spill root must not be null");
        }

        Path root = spillRoot.toPath().toRealPath();
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)
                || !Files.isWritable(root)) {
            throw new IOException("Spill root is not a writable directory: " + root);
        }

        Path namespace = createAndValidateNamespace(root);
        Path readerDirectory = namespace.resolve(readerDirectoryName(split));
        try {
            Files.createDirectory(readerDirectory);
            requireDirectory(readerDirectory, "reader workspace");
            return new DiskSpillWorkspace(readerDirectory.toRealPath());
        } catch (IOException | RuntimeException failure) {
            try {
                Files.deleteIfExists(readerDirectory);
            } catch (IOException | RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    synchronized File createSpillFile() throws IOException {
        if (deleted) {
            throw new IOException("Disk spill workspace is already deleted: " + readerDirectory);
        }

        requireDirectory(readerDirectory, "reader workspace");
        Path spill = Files.createTempFile(readerDirectory, SPILL_PREFIX, SPILL_SUFFIX);
        if (!isRegularFile(spill)) {
            throw new IOException("Failed to create a regular spill file: " + spill);
        }
        return spill.toFile();
    }

    synchronized IOException cleanup() {
        if (deleted) {
            return null;
        }
        if (Files.notExists(readerDirectory, LinkOption.NOFOLLOW_LINKS)) {
            deleted = true;
            return null;
        }
        if (Files.isSymbolicLink(readerDirectory)
                || !Files.isDirectory(readerDirectory, LinkOption.NOFOLLOW_LINKS)) {
            return new IOException("Unsafe disk spill workspace: " + readerDirectory);
        }

        try {
            // All spill files are owned and deleted by DiskSpillBufferedSplitReader before this
            // method runs. A non-empty directory is preserved fail-closed instead of deleting
            // unknown content.
            Files.delete(readerDirectory);
            deleted = true;
            return null;
        } catch (IOException | RuntimeException failure) {
            if (Files.notExists(readerDirectory, LinkOption.NOFOLLOW_LINKS)) {
                deleted = true;
                return null;
            }
            return asIOException("Failed to delete disk spill workspace", failure);
        }
    }

    synchronized boolean isDeleted() {
        return deleted;
    }

    private static Path createAndValidateNamespace(Path root) throws IOException {
        Path namespace = root.resolve(NAMESPACE);
        try {
            Files.createDirectory(namespace);
        } catch (FileAlreadyExistsException ignored) {
            // Concurrent readers may initialize the same namespace independently.
        }
        requireDirectory(namespace, "disk spill namespace");
        return namespace.toRealPath();
    }

    private static String readerDirectoryName(InputSplit split) {
        if (split == null) {
            return READER_PREFIX
                    + "session-unknown-split-unknown-attempt-"
                    + UUID.randomUUID().toString();
        }
        StringBuilder name = new StringBuilder(READER_PREFIX)
                .append("session-")
                .append(sanitize(split.getSessionId()));
        if (split instanceof InputSplitWithIndex) {
            name.append("-index-")
                    .append(((InputSplitWithIndex) split).getSplitIndex());
        }
        if (split instanceof InputSplitWithBucket) {
            name.append("-bucket-")
                    .append(((InputSplitWithBucket) split).getBucketId());
        }
        if (split instanceof InputSplitWithRowRange) {
            RowRange range = ((InputSplitWithRowRange) split).getRowRange();
            name.append("-row-")
                    .append(range.getStartIndex())
                    .append('-')
                    .append(range.getNumRecord());
        }
        return name.append("-attempt-")
                .append(UUID.randomUUID().toString())
                .toString();
    }

    private static String sanitize(String value) {
        if (value == null || value.isEmpty()) {
            return "unknown";
        }
        StringBuilder sanitized = new StringBuilder(
                Math.min(value.length(), MAX_SESSION_ID_LENGTH));
        for (int i = 0; i < value.length() && sanitized.length() < MAX_SESSION_ID_LENGTH; i++) {
            char character = value.charAt(i);
            if ((character >= 'a' && character <= 'z')
                    || (character >= 'A' && character <= 'Z')
                    || (character >= '0' && character <= '9')
                    || character == '-'
                    || character == '_'
                    || character == '.') {
                sanitized.append(character);
            } else {
                sanitized.append('_');
            }
        }
        return sanitized.length() == 0 ? "unknown" : sanitized.toString();
    }

    private static void requireDirectory(Path path, String description) throws IOException {
        if (Files.isSymbolicLink(path)
                || !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Unsafe " + description + ": " + path);
        }
    }

    private static boolean isRegularFile(Path path) {
        return !Files.isSymbolicLink(path)
                && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS);
    }

    private static IOException asIOException(String message, Throwable failure) {
        if (failure instanceof IOException) {
            return (IOException) failure;
        }
        return new IOException(message, failure);
    }
}
