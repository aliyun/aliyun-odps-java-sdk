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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.SimpleTimeZone;

import javax.mail.internet.MimeUtility;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.ExceptionCode;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;

public class LocalRunUtils {

  public static long getPID() {
    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    return Long.parseLong(processName.split("@")[0]);
  }

  public static String getCurrentTime() {
    return getDateFormat(Constants.DATE_FORMAT_2).format(new Date());
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
    }
  }

  public static void checkArgument(String name, int value, int lower_bound, int upper_bound) {
    if (value < lower_bound || value > upper_bound) {
      throw new RuntimeException("Local Run: Value of " + name
                                 + " out of bound, must be in range [" + lower_bound + ","
                                 + upper_bound + "].");
    }
  }

  public static DateFormat getDateFormat(String formateStr) {
    SimpleDateFormat dateFormat = new SimpleDateFormat(formateStr);
    return dateFormat;
  }

  /**
   * 获取某个目录下的所有空子目录，不包含该目录
   */
  public static List<File> listEmptyDirectory(File dir) {
    List<File> dataFiles = new ArrayList<File>();
    File[] subDirs = dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory() && !pathname.isHidden();
      }
    });
    for (File file : subDirs) {
      if (file.isDirectory()) {
        listEmptyDirectory(file, dataFiles);
      }
    }
    return dataFiles;
  }

  private static void listEmptyDirectory(File dir, List<File> dataFiles) {
    if (!dir.isDirectory()) {
      return;
    }
    File[] subDirs = dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return !pathname.isHidden();
      }
    });
    if (subDirs.length == 0) {
      System.out.println(dir.getAbsolutePath());
      dataFiles.add(dir);
      return;
    }
    for (File f : subDirs) {
      if (f.isDirectory()) {
        listEmptyDirectory(f, dataFiles);
      }
    }

  }

  /**
   * 获取当前目录的数据文件，不包括子目录中的数据文件
   *
   * @param dir
   * @return
   */
  public static List<File> listDataFiles(File dir) {
    List<File> dataFiles = new ArrayList<File>();
    for (File file : dir.listFiles()) {
      if (!Constants.SCHEMA_FILE.equals(file.getName()) && file.isFile()) {
        dataFiles.add(file);
      }
    }
    return dataFiles;
  }

  /**
   * 获取当前目录的数据文件，包括子目录中的数据文件
   *
   * @param srcFile
   * @param result
   */
  public static void listAllDataFiles(File srcFile, List<File> result) {

    if (srcFile.isDirectory()) {
      final Collection<File> list = FileUtils.listFiles(srcFile, HiddenFileFilter.VISIBLE,
                                                        HiddenFileFilter.VISIBLE);
      for (File f : list) {
        listAllDataFiles(f, result);
      }
    } else if (!Constants.SCHEMA_FILE.equals(srcFile.getName())) {
      result.add(srcFile);
    }
  }

  public static void removeDataFiles(File dir) {
    for (File file : listDataFiles(dir)) {
      file.delete();
    }
  }

  public static List<Integer> genReadColsIndexes(TableMeta meta, String[] readCols) {
    if (readCols == null || readCols.length == 0 || readCols[0] == null
        || readCols[0].trim().length() == 0) {
      return null;
    }
    List<Integer> indexes = new ArrayList<Integer>();
    for (String col : readCols) {
      boolean found = false;
      for (int j = 0; j < meta.getCols().length; ++j) {
        if (meta.getCols()[j].getName().equals(col)) {
          indexes.add(j);
          found = true;
          break;
        }
      }
      if (!found) {
        throw new RuntimeException(ExceptionCode.ODPS_0720021 + " - " + col + " in table "
                                   + meta.getProjName() + "." + meta.getTableName());
      }
    }
    return indexes;
  }

  public static Column[] getInputTableFields(TableMeta meta, String[] cols) {
    if (cols == null || cols[0].length() == 0) {
      return meta.getCols();
    }
    List<Column> fields = new ArrayList<Column>();
    for (String col : cols) {
      for (Column field : meta.getCols()) {
        if (col.equals(field.getName())) {
          fields.add(field);
        }
      }
    }

    return fields.toArray(new Column[fields.size()]);
  }

  public static Column[] getInputTableFields(Table table, String[] cols) {
    List<Column> schema = table.getSchema().getColumns();
    if (cols == null || cols.length == 0) {
      return null;
    }
    Column[] fields = new Column[cols.length];
    for (int i = 0; i < cols.length; i++) {
      for (Column field : schema) {
        if (schema.get(i).getName().equals(field.getName())) {
          fields[i] = schema.get(i);
        }
      }
    }
    return fields;
  }

  public static List<String> parseResourceName(String resourceName, String defaultProject)
      throws IOException {
    List<String> ret = new ArrayList<String>();
    resourceName = resourceName.trim();
    if (resourceName.isEmpty()) {
      throw new IllegalArgumentException(ExceptionCode.ODPS_0720031);
    }
    String[] arr = resourceName.split("/");
    if (arr.length == 1) {
      ret.add(defaultProject);
      ret.add(arr[0]);
    } else if (arr.length == 3) {
      if (!arr[1].equals("resources")) {
        throw new IllegalArgumentException(
            ExceptionCode.ODPS_0720041 + " - '" + resourceName + "'");
      }
      ret.add(arr[0]);
      ret.add(arr[2]);
    } else {
      throw new IllegalArgumentException(ExceptionCode.ODPS_0720041 + " - '" + resourceName + "'");
    }

    return ret;
  }

  public static String getDownloadErrorMsg(String tableInfo) {
    return "\nTable '"
           + tableInfo
           + "' is not exists in local warehouse, and download failed! Please check the following information:\n"
           + "1. Endpoint/accessId/accessKey\n" 
           + "2. Project/Table/Partition name\n"
           + "3. Local warehouse table __schema__ file infomation.\n"
           + "4. If remote server has this table.\n"
           + "Download Mode:" + WareHouse.getInstance().getDownloadMode().toString();
  }

  /**
   * 检查 relativePath是否为parent的子目录或者子文件，并且判断路径中是否存在软链接
   * 文件相等必须先CanoncialFile归一化后做判断，否则可能统一个文件两个不同路径表示，最后为不相等
   * 通过child不停的getParentFile之后和parent做比较判断来执行是否为子节点的逻辑
   * 如果child不是子节点，最后会为null
   * 
   * 
   * @param parent
   *          父目录
   * @param realativePath
   *          子目录相对路径
   * @throws IOException
   *           资源未声明、资源类型不匹配以及其他错误抛异常
   */
  private static void checkParent(File parent, String relativePath) {
    try {
      File child = new File(parent, relativePath);
      if (!child.exists()) {
        throw new IOException("ODPS-0140171: must set a correct realtive path:"  + relativePath);
      }
      while (child != null && !child.getCanonicalFile().equals(parent.getCanonicalFile())) {
        if (FileUtils.isSymlink(child)) {
          throw new IOException("ODPS-0140171: not allow symlink in archive files:" + child.getName());
        }
        child = child.getParentFile();
      }
      if (child == null) {
        throw new IOException("ODPS-0140171: not correct parameter of relative path in getCacheArchive... :" + relativePath);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      IOException tmp_e = new IOException("ODPS-0140171: permission denied to read archive resource '" + parent.getName() + "' with relative path :" + relativePath);
      throw new RuntimeException(tmp_e);
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static Collection<File> listFiles(final File dir, final String relativePath) throws IOException {
    // privileged code, for this method may be invoked by user code
    try {
      return (Collection<File>) AccessController
        .doPrivileged(new PrivilegedAction() {
            public Object run() {
              File relativeDir = new File(dir, relativePath);
              checkParent(dir, relativePath);
              if (relativeDir.isDirectory()) {
                return FileUtils.listFiles(relativeDir, new InternalIOFilter(),
                    new InternalIOFilter());
              } else {
                Collection<File> files = new java.util.LinkedList<File>();
                files.add(relativeDir);
                return files;
              }
            }
          }); 
    } catch (RuntimeException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  private static class InternalIOFilter implements IOFileFilter {
    
    public InternalIOFilter() {
      super();
    }

    @Override
    public boolean accept(File dir, String name) {
      return accept(new File(dir,name));
    }

    @Override
    public boolean accept(File file) {
      try {
        if (FileUtils.isSymlink(file)) {
          throw new IOException("ODPS-0140171: not allow symlink in archive files:" + file.getName());
        }
        return true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static String toReadableString(byte[] b) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream printableos = MimeUtility.encode(baos, "quoted-printable");
    printableos.write(b);
    printableos.close();
    return new String(baos.toByteArray(), Charset.forName("UTF-8"));
  }

  public static byte[] fromReadableString(String str) throws Exception {
    byte[] b = str.getBytes();
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    InputStream printableis = MimeUtility.decode(bais, "quoted-printable");
    byte[] tmp = new byte[b.length];
    int n = printableis.read(tmp);
    byte[] res = new byte[n];
    System.arraycopy(tmp, 0, res, 0, n);
    return res;
  }
}
