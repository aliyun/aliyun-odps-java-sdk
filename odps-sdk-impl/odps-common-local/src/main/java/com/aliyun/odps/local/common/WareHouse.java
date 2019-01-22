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

package com.aliyun.odps.local.common;

import com.aliyun.odps.local.common.utils.TypeConvertUtils;
import com.aliyun.odps.type.TypeInfo;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.utils.DownloadUtils;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.local.common.utils.PartitionUtils;
import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class WareHouse {

  private static final Log LOG = LogFactory.getLog(WareHouse.class);
  public static final Charset encoding = Charset.forName("UTF-8");

  private File warehouseDir;

  private static volatile WareHouse wareHouse;

  private ThreadLocal<Odps> odpsThreadLocal = new ThreadLocal<Odps>();

  private ThreadLocal<Configuration> confThreadLocal = new ThreadLocal<Configuration>();

  private JobDirecotry jobDirecotry;

  private WareHouse() {
    this(Constants.WAREHOUSE_HOUSE_DIR);
  }

  private WareHouse(String dir) {
    warehouseDir = new File(dir);
    if (!warehouseDir.exists()) {
      warehouseDir.mkdirs();
    }
    jobDirecotry = new JobDirecotry();
  }

  public static synchronized WareHouse getInstance() {
    if (wareHouse == null) {
      wareHouse = new WareHouse();
    }
    return wareHouse;
  }

  public static synchronized WareHouse getInstance(String warehouseDir) {
    if (wareHouse == null) {
      wareHouse = new WareHouse(warehouseDir);
    }
    return wareHouse;
  }

  public static void init(Odps odps, Configuration conf) {
    getInstance().setOdps(odps);
    getInstance().setConfiguration(conf);
  }

  public File getWarehouseDir() {
    return warehouseDir;
  }

  public File getProjectDir(String projName) {
    return new File(warehouseDir, projName);
  }
  
  /**
   * get directory: warehouse/project_name/__resources__
   * 
   */
  public File getResourceDir(String projName) {
    return new File(getProjectDir(projName), Constants.RESOURCES_DIR);
  }

  public File getTableDir(String projName, String tblName) {
    File tableDir = new File(getProjectDir(projName), Constants.TABLES_DIR + File.separator
                                                      + tblName);
    if (!tableDir.exists()) {
      // 兼容老版本warehouse目录结构
      File oldVersionDir = new File(getProjectDir(projName), tblName);
      if (oldVersionDir.exists()) {
        return oldVersionDir;
      }
    }
    return tableDir;
  }

  /**
   * if partSpec is null or empty will return table directory
   */
  public File getPartitionDir(String projName, String tblName, PartitionSpec partSpec) {
    if (partSpec == null || partSpec.isEmpty()) {
      return getTableDir(projName, tblName);
    }
    Map<PartitionSpec, File> map = getPartitionToPathMap(projName, tblName);
    for (PartitionSpec key : map.keySet()) {
      if (PartitionUtils.isEqual(key, partSpec)) {
        return map.get(key);
      }
    }
    return null;
  }

  /**
   * 从Warehouse中的数据文件路径中解析出partition信息
   *
   * @param projName
   * @param tblName
   * @param dataFile
   *     Warehouse中的数据文件
   * @return
   */
  public PartitionSpec resolvePartition(String projName, String tblName, File dataFile) {
    if (StringUtils.isBlank(projName) || StringUtils.isBlank(tblName) || dataFile == null
        || !dataFile.exists()) {
      return null;
    }
    Map<PartitionSpec, File> partitionToPathMap = getPartitionToPathMap(projName, tblName);
    String dataFilePath = dataFile.getAbsolutePath().replaceAll("\\\\", "/");
    if (!dataFilePath.endsWith("/")) {
      dataFilePath = dataFilePath + "/";
    }
    for (PartitionSpec key : partitionToPathMap.keySet()) {
      String partitionPath = partitionToPathMap.get(key).getAbsolutePath().replaceAll("\\\\", "/");
      if (dataFilePath.startsWith(partitionPath)) {
        return key;
      }
    }
    return null;
  }

  public File getReourceFile(String projName, String resourceName) {
    return new File(warehouseDir, projName + File.separator + Constants.RESOURCES_DIR
                                  + File.separator + resourceName);
  }

  public File getTableReourceFile(String projName, String resourceName) {
    File tableResourceDir = getReourceFile(projName, resourceName);
    return tableResourceDir.listFiles()[0];
  }

  public File getTableSchemeFile(String projectName, String tableName)
      throws FileNotFoundException {
    if (!existsTableSchema(projectName, tableName)) {
      throw new FileNotFoundException("Table Directory :" + projectName + "." + tableName
                                      + " Not exists in warehouse!");
    }
    File tableDir = getTableDir(projectName, tableName);
    return new File(tableDir, Constants.SCHEMA_FILE);
  }

  public String getRelativePath(String projName, String tblName, PartitionSpec partSpec,
                                Object... flag) {
    String relativePath = projName + File.separator + tblName + File.separator;
    if (partSpec != null) {
      relativePath += PartitionUtils.toString(partSpec);
    }
    return relativePath;
  }

  public List<File> getDataFiles(String projName, String tblName, PartitionSpec pattern,
                                 char inputColumnSeperator) throws IOException, OdpsException {

    if (pattern != null && !pattern.isEmpty() && !existsPartition(projName, tblName, pattern)) {
      LinkedHashMap<String, String> part = PartitionUtils.convert(pattern);
      TableInfo tableInfo = TableInfo.builder().projectName(projName).tableName(tblName)
          .partSpec(part).build();
      DownloadUtils.downloadTableSchemeAndData(WareHouse.getInstance().getOdps(), tableInfo,
        getLimitDownloadRecordCount(), inputColumnSeperator);
    } else if (!existsTable(projName, tblName)) {
      TableInfo tableInfo = TableInfo.builder().projectName(projName).tableName(tblName).build();
      DownloadUtils.downloadTableSchemeAndData(WareHouse.getInstance().getOdps(), tableInfo,
        getLimitDownloadRecordCount(), inputColumnSeperator);
    }

    File tableDir = getTableDir(projName, tblName);
    TableMeta tableMeta = SchemaUtils.readSchema(tableDir);
    boolean isPartitionTable = false;
    if (tableMeta.getPartitions() != null && tableMeta.getPartitions().length > 0) {
      isPartitionTable = true;
    }

    // not a partition table
    if (!isPartitionTable) {
      if (pattern != null && !pattern.isEmpty()) {
        throw new OdpsException("Table " + projName + "." + tblName + " is not a partition table");
      }
      return LocalRunUtils.listDataFiles(tableDir);
    }

    // get all partitions
    if (pattern == null) {
      List<File> result = new ArrayList<File>();
      LocalRunUtils.listAllDataFiles(tableDir, result);
      return result;
    }

    List<File> result = new ArrayList<File>();
    Map<PartitionSpec, File> partitionToPathMap = WareHouse.getInstance()
        .getPartitionToPathMap(projName, tblName);
    for (PartitionSpec parts : partitionToPathMap.keySet()) {
      if (PartitionUtils.match(pattern, parts)) {
        result.addAll(LocalRunUtils.listDataFiles(partitionToPathMap.get(parts)));
      }
    }

    return result;
  }

  /**
   * copy table schema to destination directory
   */
  public boolean copyTableSchema(String projectName, String tableName, File destDir,
                                 int limitDownloadRecordCount, char inputColumnSeperator)
      throws IOException, OdpsException {
    if (StringUtils.isBlank(projectName) || StringUtils.isBlank(tableName) || destDir == null) {
      return false;
    }
    TableInfo tableInfo = TableInfo.builder().projectName(projectName).tableName(tableName).build();

    LOG.info("Start to copy table schema: " + tableInfo + "-->" + destDir.getAbsolutePath());

    if (!existsTable(projectName, tableName)) {
      DownloadUtils.downloadTableSchemeAndData(getOdps(), tableInfo, limitDownloadRecordCount,
                                               inputColumnSeperator);
    }

    File tableDir = getTableDir(projectName, tableName);
    File schemaFile = new File(tableDir, Constants.SCHEMA_FILE);
    if (!schemaFile.exists()) {
      throw new FileNotFoundException("Schema file of table " + projectName + "." + tableName
                                      + " not exists in warehouse.");
    }

    if (!destDir.exists()) {
      destDir.mkdirs();
    }

    FileUtils.copyFileToDirectory(schemaFile, destDir);
    LOG.info("Finished copy table schema: " + tableInfo + "-->" + destDir.getAbsolutePath());
    return true;
  }

  /**
   * copy table schema and partition data from warehouse to target directory ,
   * if partition not exists will download the partition data and table schema
   * from remote server
   *
   * @param projectName
   * @param tableName
   * @param partSpec
   *     if null will copy all partitions
   * @param readCols
   *     if null will copy all columns
   * @param destDir
   * @return
   * @throws IOException
   * @throws OdpsException
   */
  public boolean copyTable(String projectName, String tableName, PartitionSpec partSpec,
                           String[] readCols, File destDir, int limitDownloadRecordCount,
                           char inputColumnSeperator) {

    if (StringUtils.isBlank(projectName) || StringUtils.isBlank(tableName) || destDir == null) {
      return false;
    }
    TableInfo tableInfo = TableInfo.builder().projectName(projectName).tableName(tableName)
        .partSpec(partSpec).build();

    LOG.info("Start to copy table: " + tableInfo + "-->" + destDir.getAbsolutePath());

    boolean hasPartition = false;
    if (partSpec != null && !partSpec.isEmpty()) {
      hasPartition = true;
    }

    // if not exist table, then download from odps server
    if (hasPartition && !existsPartition(projectName, tableName, partSpec)) {
      DownloadUtils.downloadTableSchemeAndData(getOdps(), tableInfo, limitDownloadRecordCount,
                                               inputColumnSeperator);
    } else if (!existsTable(projectName, tableName)) {
      DownloadUtils.downloadTableSchemeAndData(getOdps(), tableInfo, limitDownloadRecordCount,
                                               inputColumnSeperator);
    }

    File whTableDir = getTableDir(projectName, tableName);

    // copy schema file
    File schemaFile = new File(whTableDir, Constants.SCHEMA_FILE);
    if (!schemaFile.exists()) {
      throw new RuntimeException("Schema file of table " + projectName + "." + tableName
                                 + " not exists in warehouse.");
    }

    if (!destDir.exists()) {
      destDir.mkdirs();
    }

    // copy table schema file
    try {
      FileUtils.copyFileToDirectory(schemaFile, destDir);
    } catch (IOException e) {
      throw new RuntimeException("Copy schema file of table " + tableInfo + " failed!"
                                 + e.getMessage());
    }

    // copy partition data files
    TableMeta tableMeta = getTableMeta(projectName, tableName);
    List<Integer> indexes = LocalRunUtils.genReadColsIndexes(tableMeta, readCols);

    if (hasPartition) {
      final Collection<File> dataFiles = FileUtils.listFiles(whTableDir, HiddenFileFilter.VISIBLE,
                                                             HiddenFileFilter.VISIBLE);
      for (File dataFile : dataFiles) {
        if (dataFile.getName().equals(Constants.SCHEMA_FILE)) {
          continue;
        }
        String parentDir = dataFile.getParentFile().getAbsolutePath();

        String partPath = parentDir.substring(whTableDir.getAbsolutePath().length(),
                                              parentDir.length());
        PartitionSpec ps = PartitionUtils.convert(partPath);
        if (PartitionUtils.isEqual(ps, partSpec)) {
          File destPartitionDir = new File(destDir, PartitionUtils.toString(ps));
          destPartitionDir.mkdirs();
          try {
            copyDataFiles(dataFile.getParentFile(), indexes, destPartitionDir,
                          inputColumnSeperator);
          } catch (IOException e) {
            throw new RuntimeException("Copy data file of table " + tableInfo + " failed!"
                                       + e.getMessage());
          }
        }

      }
    } else {
      try {
        copyDataFiles(whTableDir, indexes, destDir, inputColumnSeperator);
      } catch (IOException e) {
        throw new RuntimeException("Copy data file of table " + tableInfo + " failed!"
                                   + e.getMessage());
      }

    }

    LOG.info("Finished copy table: " + tableInfo + "-->" + destDir.getAbsolutePath());

    return true;

  }

  /**
   * copy resource from warehouse/__resources__/ to temp/resource/
   *
   * @param projName
   * @param resourceName
   * @param resourceRootDir
   * @param limitDownloadRecordCount
   * @param inputColumnSeperator
   * @throws IOException
   * @throws OdpsException
   */
  public void copyResource(String projName, String resourceName, File resourceRootDir,
                           int limitDownloadRecordCount, char inputColumnSeperator)
      throws IOException, OdpsException {
    if (StringUtils.isBlank(projName) || StringUtils.isBlank(resourceName)
        || resourceRootDir == null) {
      return;
    }

    if (!resourceRootDir.exists()) {
      resourceRootDir.mkdirs();
    }

    LOG.info("Start to copy resource: " + projName + "." + resourceName + "-->"
             + resourceRootDir.getAbsolutePath());

    if (!existsResource(projName, resourceName)) {
      DownloadUtils.downloadResource(getOdps(), projName, resourceName, limitDownloadRecordCount,
                                     inputColumnSeperator);
    }

    File file = getReourceFile(projName, resourceName);

    // table resource
    if (file.isDirectory()) {
      File tableResourceDir = new File(resourceRootDir, resourceName);

      TableInfo refTableInfo = getReferencedTable(projName, resourceName);
      LinkedHashMap<String, String> partitions = refTableInfo.getPartSpec();

      if (partitions != null && partitions.size() > 0) {
        PartitionSpec partSpec = new PartitionSpec();
        for (String key : partitions.keySet()) {
          partSpec.set(key, partitions.get(key));
        }
        copyTable(refTableInfo.getProjectName(), refTableInfo.getTableName(), partSpec, null,
                  tableResourceDir, limitDownloadRecordCount, inputColumnSeperator);
      } else {
        copyTable(refTableInfo.getProjectName(), refTableInfo.getTableName(), null, null,
                  tableResourceDir, limitDownloadRecordCount, inputColumnSeperator);
      }

    } else {
      // not table resource
      if (!existsResource(projName, resourceName)) {

        DownloadUtils.downloadResource(getOdps(), projName, resourceName, limitDownloadRecordCount,
                                       inputColumnSeperator);
      }
      FileUtils.copyFileToDirectory(file, resourceRootDir);
    }

    LOG.info("Finished copy resource: " + projName + "." + resourceName + "-->"
             + resourceRootDir.getAbsolutePath());
  }

  /**
   * copy output data files from job directory to warehouse
   *
   * @param srcDir
   * @param indexes
   * @param destDir
   * @throws IOException
   */
  public void copyDataFiles(File srcDir, List<Integer> indexes, File destDir,
                            char inputColumnSeperator) throws IOException {
    if (indexes == null || indexes.isEmpty()) {
      for (File file : LocalRunUtils.listDataFiles(srcDir)) {
        FileUtils.copyFileToDirectory(file, destDir);
      }
    } else {
      for (File file : LocalRunUtils.listDataFiles(srcDir)) {
        CsvReader reader = DownloadUtils.newCsvReader(
            file.getAbsolutePath(), inputColumnSeperator, encoding);
        CsvWriter writer = new CsvWriter(new File(destDir, file.getName()).getAbsolutePath(),
                                         inputColumnSeperator, encoding);
        while (reader.readRecord()) {
          String[] vals = reader.getValues();
          String[] newVals = new String[indexes.size()];
          for (int i = 0; i < indexes.size(); ++i) {
            newVals[i] = vals[indexes.get(i)];
          }
          writer.writeRecord(newVals);
        }
        writer.close();
        reader.close();
      }
    }
  }

  public File createPartitionDir(String projName, String tblName, PartitionSpec partSpec) {
    File tableDir = getTableDir(projName, tblName);
    if (!tableDir.exists()) {
      tableDir.mkdirs();
    }
    if (partSpec != null && !partSpec.isEmpty()) {
      File partitionDir = new File(tableDir, PartitionUtils.toString(partSpec));
      if (!partitionDir.exists()) {
        partitionDir.mkdirs();
      }
      return partitionDir;
    } else {
      return tableDir;
    }
  }

  public boolean createTableReourceFile(String projName, String resourceName,
                                        TableInfo refTableInfo) {
    StringBuffer sb = new StringBuffer();
    sb.append(refTableInfo.getProjectName());
    sb.append(".");
    sb.append(refTableInfo.getTableName());
    String partitions = refTableInfo.getPartPath();
    if (partitions != null && !partitions.trim().isEmpty()) {
      sb.append("(");
      String partStr = partitions.replaceAll("/", ",");
      if (partStr.endsWith(",")) {
        partStr = partStr.substring(0, partStr.length() - 1);
      }
      sb.append(partStr);
      sb.append(")");
    }

    File tableResourceDir = getReourceFile(projName, resourceName);
    if (!tableResourceDir.exists()) {
      tableResourceDir.mkdirs();
    }
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(new File(tableResourceDir, "__ref__"));
      pw.println(sb.toString());
      return true;
    } catch (FileNotFoundException e) {
    } finally {
      if (pw != null) {
        pw.close();
      }
    }

    return false;
  }

  public TableMeta getResourceSchema(String projName, String resourceName) throws IOException {
    File dir = getReourceFile(projName, resourceName);
    if (SchemaUtils.existsSchemaFile(dir)) {
      return SchemaUtils.readSchema(dir);
    }
    return null;
  }

  public TableMeta getTableMeta(String projName, String tblName) {
    if (StringUtils.isBlank(projName) || StringUtils.isBlank(tblName)) {
      return null;
    }
    File dir = getTableDir(projName, tblName);
    TableMeta meta = SchemaUtils.readSchema(dir);
    if (meta.getProjName() != null && !meta.getProjName().equals(projName)) {
      throw new RuntimeException("Invalid project name " + meta.getProjName()
                                 + " in file 'warehouse" + File.separator + projName
                                 + File.separator + tblName
                                 + File.separator + Constants.SCHEMA_FILE + "'");
    }
    if (meta.getTableName() != null && !meta.getTableName().equals(tblName)) {
      throw new RuntimeException("Invalid table name " + meta.getProjName() + " in file 'warehouse"
                                 + File.separator + projName + File.separator + tblName
                                 + File.separator
                                 + Constants.SCHEMA_FILE + "'");
    }
    return meta;
  }

  public Map<PartitionSpec, File> getPartitionToPathMap(String projName, String tblName) {
    File tableDir = getTableDir(projName, tblName);
    TableMeta tableMeta = SchemaUtils.readSchema(tableDir);
    Map<PartitionSpec, File> result = new HashMap<PartitionSpec, File>();
    File dir = getTableDir(projName, tblName);
    final Collection<File> dataFiles = FileUtils.listFiles(dir, HiddenFileFilter.VISIBLE,
                                                           HiddenFileFilter.VISIBLE);

    List<File> emptyPatitions = LocalRunUtils.listEmptyDirectory(dir);
    dataFiles.addAll(emptyPatitions);

    for (File dataFile : dataFiles) {
      if (dataFile.getName().equals(Constants.SCHEMA_FILE)) {
        continue;
      }

      String partPath = null;
      if (dataFile.isFile()) {
        String parentDir = dataFile.getParentFile().getAbsolutePath();
        partPath = parentDir.substring(dir.getAbsolutePath().length(), parentDir.length());
      } else {
        // empty partition directory
        String parentDir = dataFile.getAbsolutePath();
        partPath = parentDir.substring(dir.getAbsolutePath().length(), parentDir.length());
      }

      try {
        if (partPath.length() > 0) {
          PartitionSpec ps = PartitionUtils.convert(partPath);
          if (PartitionUtils.valid(tableMeta.getPartitions(), ps)) {
            result.put(ps, dataFile.getParentFile());
          }
        }
      } catch (Exception ex) {
        // LOG.warn("ignore dir: " + parentDir + ", " + ex.getMessage());
      }
    }
    return result;
  }

  public List<PartitionSpec> getPartitions(String projName, String tblName) {
    Map<PartitionSpec, File> partitionToPathMap = getPartitionToPathMap(projName, tblName);
    List<PartitionSpec> result = new ArrayList<PartitionSpec>();
    for (PartitionSpec key : partitionToPathMap.keySet()) {
      result.add(key);
    }
    return result;
  }

  public List<String> getProjectNames() {
    File warehouseDir = getWarehouseDir();
    if (!warehouseDir.exists()) {
      return null;
    }
    List<String> result = new ArrayList<String>();
    File[] projects = warehouseDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory() && !pathname.isHidden();

      }
    });
    for (File p : projects) {
      if (p.isDirectory()) {
        result.add(p.getName());
      }
    }
    return result;
  }

  public List<TableMeta> getTableMetas(String projName) throws IOException {
    File projectDir = getProjectDir(projName);
    if (!projectDir.exists()) {
      return null;
    }

    List<TableMeta> result = new ArrayList<TableMeta>();
    File[] tables = projectDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory() && !pathname.isHidden()
               && !pathname.getName().equals(Constants.RESOURCES_DIR);
      }
    });
    // old version
    for (File t : tables) {
      if (!existsTable(projName, t.getName())) {
        continue;
      }

      TableMeta tableMeta = getTableMeta(projName, t.getName());
      if (tableMeta != null) {
        result.add(tableMeta);
      }
    }

    // new version >=0.14
    File tableBaseDir = new File(projectDir, Constants.TABLES_DIR);
    if (!tableBaseDir.exists()) {
      return result;
    }
    tables = tableBaseDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory() && !pathname.isHidden();
      }
    });
    for (File t : tables) {
      if (!existsTable(projName, t.getName())) {
        continue;
      }

      TableMeta tableMeta = getTableMeta(projName, t.getName());
      if (tableMeta != null) {
        result.add(tableMeta);
      }
    }

    return result;
  }

  public List<String> getTableNames(String projName) throws IOException {
    List<TableMeta> list = getTableMetas(projName);
    if (list == null || list.size() == 0) {
      return null;
    }
    List<String> result = new ArrayList<String>();
    for (TableMeta tableMeta : list) {
      result.add(tableMeta.getTableName());
    }
    return result;
  }

  /*
   * get referenced table of table resource
   */
  public TableInfo getReferencedTable(String projName, String resourceName) {
    File file = getTableReourceFile(projName, resourceName);
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      String line = br.readLine();
      while (line != null && (line.trim().isEmpty() || line.startsWith("#"))) {
        line = br.readLine();
      }

      if (line == null || line.trim().isEmpty()) {
        return null;
      }

      String project;

      int index = line.indexOf(".");
      if (index == -1) {
        project = projName;
      } else {
        project = line.substring(0, index);
        line = line.substring(index + 1);
      }

      String table;
      index = line.indexOf("(");
      if (index == -1) {
        table = line;
        line = null;
      } else {
        table = line.substring(0, index);
        line = line.substring(index + 1, line.length() - 1);
      }

      String partitions = null;
      if (line != null) {
        partitions = "";
        String[] parts = line.split(",");
        for (String item : parts) {
          if (!partitions.equals("")) {
            partitions += "/";
          }
          partitions += item;
        }
      }

      TableInfo tableInfo;
      if (partitions == null) {
        tableInfo = TableInfo.builder().projectName(project).tableName(table).build();
      } else {
        tableInfo = TableInfo.builder().projectName(project).tableName(table).partSpec(partitions)
            .build();
      }
      return tableInfo;

    } catch (IOException e) {
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
        }
      }
    }
    return null;

  }

  public boolean existsTable(String projName, String tblName) {
    return existsTableSchema(projName, tblName);
  }

  public boolean existsTableSchema(String projectName, String tableName) {
    File tableDir = getTableDir(projectName, tableName);
    if (!tableDir.exists()) {
      return false;
    }
    return new File(tableDir, Constants.SCHEMA_FILE).exists();
  }

  public boolean existsPartition(String projectName, String tableName, PartitionSpec partSpec) {

    if (!existsTable(projectName, tableName)) {
      return false;
    }

    if (partSpec == null || partSpec.isEmpty()) {
      return true;
    }

    List<PartitionSpec> partionList = getPartitions(projectName, tableName);
    if (partionList == null || partionList.size() == 0) {
      return false;
    }

    for (PartitionSpec item : partionList) {
      if (PartitionUtils.match(partSpec, item)) {
        return true;
      }

    }
    return false;

  }

  public boolean existsResource(String projName, String resourceName) {
    return getReourceFile(projName, resourceName).exists();
  }

  /**
   * will drop table directory,include table schema and data
   */
  public void dropTableIfExists(String projName, String tblName) throws IOException {
    File tableDir = getTableDir(projName, tblName);
    if (tableDir != null && tableDir.exists() && tableDir.isDirectory()) {
      FileUtils.deleteDirectory(tableDir);
    }
  }

  /**
   * drop partition data if exists, will not drop table schema if partSpec is
   * null, will drop table data
   */
  public void dropTableDataIfExists(String projName, String tblName, PartitionSpec partSpec)
      throws IOException {
    File partitionDir = getPartitionDir(projName, tblName, partSpec);
    if (partitionDir != null && partitionDir.exists() && partitionDir.isDirectory()) {
      LocalRunUtils.removeDataFiles(partitionDir);
    }
  }

  /**
   * if partSpec is null will check if table has no data file else will check
   * the partition has no data file
   */
  public boolean isTableEmpty(String projName, String tblName, PartitionSpec partSpec)
      throws IOException {
    File partitionDir = getPartitionDir(projName, tblName, partSpec);
    if (partitionDir != null && partitionDir.exists() && partitionDir.isDirectory()) {
      List<File> dataFiles = new ArrayList<File>();
      LocalRunUtils.listAllDataFiles(partitionDir, dataFiles);
      if (dataFiles == null || dataFiles.isEmpty()) {
        return true;
      }
      // empty data file
      for (File f : dataFiles) {
        if (f.length() > 0) {
          return false;
        }
      }
    }
    return true;
  }

  public void dropResourceIfExists(String projName, String resourceName) throws IOException {
    File resourceDir = getReourceFile(projName, resourceName);
    if (resourceDir == null || !resourceDir.exists()) {
      return;
    }
    if (resourceDir.isDirectory()) {
      FileUtils.deleteDirectory(resourceDir);
    }
    if (resourceDir.isFile()) {
      resourceDir.delete();
    }
  }

  public boolean valid(String projName, String tblName, PartitionSpec partitionSpec,
                       String[] readCols) throws OdpsException, IOException {
    if (StringUtils.isBlank(projName)) {
      throw new OdpsException("Project " + projName + " is null");
    }
    if (StringUtils.isBlank(tblName)) {
      throw new OdpsException("Table Name is null");
    }
    if (!existsTable(projName, tblName)) {
      throw new OdpsException("table " + projName + "." + tblName + " not exitsts");
    }

    if (partitionSpec != null && !existsPartition(projName, tblName, partitionSpec)) {
      throw new OdpsException("table " + projName + "." + tblName + "("
                              + PartitionUtils.toString(partitionSpec) + ") not exitsts");
    }

    if (readCols != null) {
      TableMeta tableMeta = getTableMeta(projName, tblName);
      int columnCount = tableMeta.getCols().length;
      for (int i = 0; i < readCols.length; i++) {
        boolean isFind = false;
        for (int j = 0; j < columnCount; j++) {
          if (tableMeta.getCols()[j].getName().equals(readCols[i])) {
            isFind = true;
            break;
          }
        }
        if (!isFind) {
          throw new OdpsException("table " + projName + "." + tblName + " do not have column :"
                                  + readCols[i]);
        }
      }
    }
    return true;
  }

  public List<Object[]> readData(String projName, String tblName, PartitionSpec partitionSpec,
                                 String[] readCols, char inputColumnSeparator)
      throws OdpsException, IOException {
    List<File> dataFiles = getDataFiles(projName, tblName, partitionSpec, inputColumnSeparator);
    if (dataFiles == null || dataFiles.size() == 0) {
      return null;
    }
    File tableDir = getTableDir(projName, tblName);
    TableMeta tableMeta = SchemaUtils.readSchema(tableDir);
    List<ColumnOrConstant> columnOrConstants = SchemaUtils.parseColumnConstant(readCols, tableMeta);
    CsvReader reader;
    List<Object[]> result = new ArrayList<Object[]>();
    for (File file : dataFiles) {
      reader = DownloadUtils.newCsvReader(file.getAbsolutePath(), inputColumnSeparator, encoding);
      while (reader.readRecord()) {
        String[] vals = reader.getValues();
        Object[] newVals;
        if (columnOrConstants != null && !columnOrConstants.isEmpty()) {
          newVals = new Object[columnOrConstants.size()];
          for (int i = 0; i < columnOrConstants.size(); ++i) {
            ColumnOrConstant columnOrConstant = columnOrConstants.get(i);
            if (columnOrConstant.isConstant()) {
              newVals[i] = columnOrConstant.getConstantValue();
            } else {
              Integer colIndex = columnOrConstant.getColIndex();
              newVals[i] = TypeConvertUtils.fromString(tableMeta.getCols()[colIndex].getTypeInfo(),
                vals[colIndex], false);
            }
          }
        } else {
          newVals = new Object[vals.length];
          for (int i = 0; i < vals.length; i++) {
            newVals[i] = TypeConvertUtils
              .fromString(tableMeta.getCols()[i].getTypeInfo(), vals[i], false);
          }
        }
        result.add(newVals);
      }
      reader.close();
    }
    return result;
  }

  public Class[] getColumnTypes(String projName, String tblName, String[] readCols) {
    File tableDir = getTableDir(projName, tblName);
    TableMeta tableMeta = SchemaUtils.readSchema(tableDir);
    Column[] columns = tableMeta.getCols();
    List<ColumnOrConstant> columnOrConstants = SchemaUtils.parseColumnConstant(readCols, tableMeta);
    Class[] result;
    if (columnOrConstants != null && !columnOrConstants.isEmpty()) {
      result = new Class[columnOrConstants.size()];
      for (int i = 0; i < columnOrConstants.size(); i++) {
        ColumnOrConstant columnOrConstant = columnOrConstants.get(i);
        TypeInfo typeInfo;
        if (columnOrConstant.isConstant()) {
          typeInfo = columnOrConstant.getConstantTypeInfo();
        } else {
          Integer colIndex = columnOrConstant.getColIndex();
          typeInfo = columns[colIndex].getTypeInfo();
        }
        result[i] = TypeConvertUtils.getOdpsJavaType(typeInfo);
      }
    } else {
      result = new Class[columns.length];
      for (int i = 0; i < columns.length; i++) {
        result[i] = TypeConvertUtils.getOdpsJavaType(columns[i].getTypeInfo());
      }
    }

    return result;
  }

  /**
   * when finished read should close inputstream
   */
  public BufferedInputStream readResourceFileAsStream(String project, String resource,
                                                      char inputColumnSeperator)
      throws IOException, OdpsException {

    if (!existsResource(project, resource)) {
      DownloadUtils.downloadResource(WareHouse.getInstance().getOdps(), getOdps()
          .getDefaultProject(), resource, getLimitDownloadRecordCount(), inputColumnSeperator);
    }

    if (!existsResource(project, resource)) {
      throw new OdpsException("File Resource " + project + "." + resource + " not exists");
    }

    File file = getReourceFile(project, resource);
    if (!file.isFile()) {
      throw new OdpsException("Resource " + project + "." + resource
                              + " is not a valid file Resource, because it is a direcotry");
    }
    return new BufferedInputStream(new FileInputStream(file));
  }

  public byte[] readResourceFile(String project, String resource, char inputColumnSeperator)
      throws IOException, OdpsException {
    if (!existsResource(project, resource)) {
      DownloadUtils.downloadResource(WareHouse.getInstance().getOdps(), getOdps()
          .getDefaultProject(), resource, getLimitDownloadRecordCount(), inputColumnSeperator);
    }

    File file = getReourceFile(project, resource);
    if (!file.isFile()) {
      throw new OdpsException("Resource " + project + "." + resource
                              + " is not a valid file Resource, because it is a direcotry");
    }
    FileInputStream in = new FileInputStream(file);
    ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
    byte[] temp = new byte[1024];
    int length;
    while ((length = in.read(temp)) != -1) {
      out.write(temp, 0, length);
    }
    in.close();
    return out.toByteArray();
  }

  public Iterator<Object[]> readResourceTable(String project, String resource,
                                              final char inputColumnSeperator)
      throws IOException, OdpsException {
    if (!existsResource(project, resource)) {
      DownloadUtils.downloadResource(WareHouse.getInstance().getOdps(), getOdps()
          .getDefaultProject(), resource, getLimitDownloadRecordCount(), inputColumnSeperator);
    }

    File tableResourceDir = getReourceFile(project, resource);
    if (!tableResourceDir.isDirectory()) {
      throw new OdpsException("Resource " + project + "." + resource
                              + " is not a valid file Resource, because it is not a direcotry");
    }

    // LOG.info("Reading resource table from " +
    // tableResourceDir.getAbsolutePath());
    TableInfo tableInfo = getReferencedTable(project, resource);
    PartitionSpec partitionSpec = PartitionUtils.convert(tableInfo.getPartSpec());

    final List<File> datafiles = getDataFiles(project, tableInfo.getTableName(), partitionSpec,
                                              inputColumnSeperator);

    final Column[] schema = SchemaUtils.readSchema(getTableDir(project, tableInfo.getTableName()))
        .getCols();

    return new Iterator<Object[]>() {
      CsvReader reader;
      Object[] current;
      boolean fetched;

      @Override
      public boolean hasNext() {
        if (fetched) {
          return current != null;
        }
        // Fetch new one
        try {
          fetch();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return current != null;

      }

      private void fetch() throws IOException {

        // first time
        if (reader == null) {
          if (datafiles.isEmpty()) {
            current = null;
            fetched = true;
            return;
          }

          File f = datafiles.remove(0);
          reader = DownloadUtils.newCsvReader(f.getAbsolutePath(), inputColumnSeperator, encoding);
          reader.setSafetySwitch(false);
          current = read();
          fetched = true;
          return;
        }

        current = read();

        if (current == null && !datafiles.isEmpty()) {
          File f = datafiles.remove(0);
          reader = DownloadUtils.newCsvReader(f.getAbsolutePath(), inputColumnSeperator, encoding);
          reader.setSafetySwitch(false);

          current = read();

          fetched = true;
          return;
        }

        fetched = true;
      }

      @Override
      public Object[] next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        fetched = false;
        return current;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      private Object[] read() throws IOException {
        Object[] result;
        if (!reader.readRecord()) {
          return null;
        }
        String[] vals = reader.getValues();
        if (vals == null || vals.length == 0) {
          result = null;
        } else {
          result = new Object[vals.length];
          for (int i = 0; i < vals.length; i++) {
            result[i] = TypeConvertUtils.fromString(schema[i].getTypeInfo(), vals[i], false);
          }
        }
        return result;
      }

    };
  }

  // ////////////////Local Configuration////////////////////////////

  public boolean isRetainTempData() {
    String tempRetain = getConfiguration().get(Constants.LOCAL_TEMP_RETAIN, "true");
    return tempRetain.equalsIgnoreCase("false") ? false : true;
  }

  public String getJobDirStr() {
    return getConfiguration().get(Constants.LOCAL_TEMP_DIR, "temp");
  }

  public JobDirecotry getJobDir() {
    return jobDirecotry;
  }

  public String getJobName() {
    return getConfiguration().get(Constants.JOB_NAME, "");
  }

  public void setJobName(String jobname) {
    getConfiguration().set(Constants.JOB_NAME, jobname);
  }

  public char getInputColumnSeperator() {
    String seperator = getConfiguration().get(Constants.LOCAL_INPUT_COLUMN_SEPERATOR, ",");
    if (seperator.length() > 0) {
      char c = seperator.charAt(0);
      if (c == 's' || c == ' ') {
        return ' ';
      } else if (c == 't' || c == '\t') {
        return '\t';
      } else if (c == ',' || c == '\t' || c == ';' || c == '|') {
        return c;
      }
    }
    return ',';
  }

  public char getOutputColumnSeperator() {
    String seperator = getConfiguration().get(Constants.LOCAL_OUTPUT_COLUMN_SEPERATOR, ",");
    if (seperator.length() > 0) {
      char c = seperator.charAt(0);
      if (c == 's' || c == ' ') {
        return ' ';
      } else if (c == 't' || c == '\t') {
        return '\t';
      } else if (c == ',' || c == '\t' || c == ';' || c == '|') {
        return c;
      }
    }
    return ',';
  }

  public int getLimitDownloadRecordCount() {
    int limit = getConfiguration().getInt(Constants.LOCAL_RECORD_LIMIT,
                                          Constants.DEFAULT_DOWNLOAD_RECORD);
    return limit > 0 ? limit : Constants.DEFAULT_DOWNLOAD_RECORD;
  }
  
  public DownloadMode getDownloadMode() {
    String downloadModeStr =
        getConfiguration().get(Constants.LOCAL_DOWNLOAD_MODE, DownloadMode.AUTO.toString())
            .toUpperCase();
    DownloadMode downloadMode = DownloadMode.AUTO;
    try {
      downloadMode = DownloadMode.valueOf(downloadModeStr);
    } catch (Exception exception) {
    }
    return downloadMode;
  }

  public boolean caintainsKey(String key) {
    Iterator<Entry<String, String>> it = getConfiguration().iterator();
    while (it.hasNext()) {
      Entry<String, String> entry = it.next();
      if (entry.getKey().equals(key)) {
        return true;
      }
    }
    return false;
  }

  public void setOdps(Odps odps) {
    if (odps != null) {
      odpsThreadLocal.remove();
      odpsThreadLocal.set(odps);
    }
  }

  public Odps getOdps() {
    return odpsThreadLocal.get();
  }

  public void setConfiguration(Configuration conf) {
    if (conf != null) {
      confThreadLocal.remove();
      confThreadLocal.set(conf);
    }
  }

  public Configuration getConfiguration() {
    Configuration conf = confThreadLocal.get();
    if (conf == null) {
      conf = new Configuration();
      confThreadLocal.set(conf);
    }
    return conf;
  }

  public void setTunnelEndpoint(String tunnelEndpoint) {
    getConfiguration().set(Constants.TUNNEL_ENDPOINT, tunnelEndpoint);
  }

  public String getTunnelEndpoint() {
    return getConfiguration().get(Constants.TUNNEL_ENDPOINT);
  }

  public void setRecordLimit(String recordLimit) {
    if (StringUtils.isBlank(recordLimit)) {
      return;
    }
    getConfiguration().set(Constants.LOCAL_RECORD_LIMIT, recordLimit);
  }

  public void setColumnSeparator(String columnSeparator) {
    if (StringUtils.isBlank(columnSeparator)) {
      return;
    }
    getConfiguration().set(Constants.LOCAL_INPUT_COLUMN_SEPERATOR, columnSeparator);
  }
}
