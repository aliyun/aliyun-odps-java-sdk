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

package com.aliyun.odps.local.common.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

public class ArchiveUtils {

  public static void unArchive(File inFile, File outDir) throws IOException {
    try {
      String name = inFile.getName().toLowerCase();
      if (name.endsWith(".zip")) {
        unZip(inFile, outDir);
      } else if (name.endsWith(".jar")) {
        unJar(inFile, outDir);
      } else if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
        unGZip(inFile, outDir);
      } else if (name.endsWith(".tar")) {
        unTar(inFile, outDir);
      }
    } catch (ArchiveException ex) {
      throw new IOException(ex);
    }
  }

  @SuppressWarnings("rawtypes")
  public static void unJar(File jarFile, File toDir) throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = (JarEntry) entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create "
                    + file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      jar.close();
    }
  }

  /**
   * Given a File input it will unzip the file in a the unzip directory passed
   * as the second parameter
   * 
   * @param inFile
   *          The zip file as input
   * @param unzipDir
   *          The unzip directory where to unzip the zip file.
   * @throws IOException
   */
  public static void unZip(File inFile, File unzipDir) throws IOException {
    Enumeration<? extends ZipEntry> entries;
    ZipFile zipFile = new ZipFile(inFile);

    try {
      entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = zipFile.getInputStream(entry);
          try {
            File file = new File(unzipDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create "
                    + file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      zipFile.close();
    }
  }

  public static void unTar(File inFile, File untarDir) throws IOException,
      ArchiveException {

    final InputStream is = new FileInputStream(inFile);
    final TarArchiveInputStream in = (TarArchiveInputStream) new ArchiveStreamFactory()
        .createArchiveInputStream(ArchiveStreamFactory.TAR, is);
    TarArchiveEntry entry = null;
    untarDir.mkdirs();
    while ((entry = (TarArchiveEntry) in.getNextEntry()) != null) {
      byte[] content = new byte[(int) entry.getSize()];
      in.read(content);
      final File entryFile = new File(untarDir, entry.getName());
      if (entry.isDirectory() && !entryFile.exists()) {
        if (!entryFile.mkdirs()) {
          throw new IOException("Create directory failed: "
              + entryFile.getAbsolutePath());
        }
      } else {
        final OutputStream out = new FileOutputStream(entryFile);
        IOUtils.write(content, out);
        out.close();
      }
    }
    in.close();
  }

  public static void unGZip(File gzFile, File ungzipDir) throws IOException,
      ArchiveException {
    String gzFileName = gzFile.getName();
    String tarFileName = "";
    if (gzFileName.endsWith(".tar.gz")) {
      tarFileName = gzFileName.substring(0, gzFileName.length() - 3);// 3 is length of '.gz'
    } else if (gzFileName.endsWith(".tgz")) {
      tarFileName = gzFileName.substring(0, gzFileName.length() - 4) + ".tar";// 4 is length of
                                                                              // '.tgz'
    }
    File tarFile = new File(gzFile.getParentFile(), tarFileName);
    FileOutputStream tarOut = new FileOutputStream(tarFile);
    GzipCompressorInputStream gzIn = new GzipCompressorInputStream(
        new BufferedInputStream(new FileInputStream(gzFile)));
    IOUtils.copy(gzIn, tarOut);
    tarOut.close();
    gzIn.close();

    unTar(tarFile, ungzipDir);
    // remove the temporary tarFile
    FileUtils.deleteQuietly(tarFile);
  }
}
