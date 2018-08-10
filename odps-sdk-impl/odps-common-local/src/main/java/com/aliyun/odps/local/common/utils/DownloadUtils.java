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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;

import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.DownloadMode;
import com.aliyun.odps.local.common.ExceptionCode;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class DownloadUtils {

  private static final Log LOG = LogFactory.getLog(DownloadUtils.class);
  public static final Charset encoding = Charset.forName("UTF-8");

  public static void downloadTableSchemeAndData(Odps odps, TableInfo tableInfo,
                                                int limitDownloadRecordCount,
                                                char inputColumnSeperator) {

    if (odps == null || tableInfo == null) {
      throw new IllegalArgumentException("Missing arguments:odps|tableInfo");
    }
    
    if (StringUtils.isBlank(tableInfo.getProjectName())) {
      tableInfo.setProjectName(odps.getDefaultProject());
    }

    validateTable(odps, tableInfo);

    Table table = odps.tables().get(tableInfo.getProjectName(), tableInfo.getTableName());

    // create table scheme file
    File tableDir = WareHouse.getInstance().getTableDir(tableInfo.getProjectName(),
                                                        tableInfo.getTableName());
    if (!tableDir.exists()){
      tableDir.mkdirs();
    }
    TableMeta tableMeta = TableMeta.fromTable(table);
    SchemaUtils.generateSchemaFile(tableMeta, null, tableDir);

    // download data file
    if (tableMeta.getPartitions() != null && tableMeta.getPartitions().length > 0) {
      LinkedHashMap<String, String> parts = tableInfo.getPartSpec();
      int downloadedPartition = 0;
      for (Partition partition : table.getPartitions()) {
        PartitionSpec spec = partition.getPartitionSpec();
        if (matches(spec, parts)) {
          downloadTable(odps, tableMeta, spec, limitDownloadRecordCount, inputColumnSeperator);
          downloadedPartition++;
        }
      }
      if (downloadedPartition == 0) {
        throw new RuntimeException("No partition found for " + tableInfo.toString());
      }
    } else {
      if (tableInfo.getPartSpec().size() > 0) {
        throw new RuntimeException("Warehouse not exists table: " + tableInfo
                                   + " ,while try to download from remote server found it is not a partitioned table");
      }
      downloadTable(odps, tableMeta, null, limitDownloadRecordCount, inputColumnSeperator);
    }

  }

  public static File downloadTable(Odps odps, TableMeta tableMeta, PartitionSpec partition,
                                   int limitDownloadRecordCount, char inputColumnSeperator) {

    TableInfo tableInfo = TableInfo.builder().projectName(tableMeta.getProjName())
        .tableName(tableMeta.getTableName()).partSpec(partition).build();

    List<String[]> records = downloadTableData(odps, tableMeta.getProjName(),
                                               tableMeta.getTableName(), partition,
                                               limitDownloadRecordCount, null);

    File tableDir = WareHouse.getInstance().getTableDir(tableMeta.getProjName(),
                                                        tableMeta.getTableName());
    if (!tableDir.exists()) {
      tableDir.mkdirs();
    }

    File dataDir = tableDir;
    if (partition != null) {
      dataDir = new File(tableDir, PartitionUtils.toString(partition));
      if (!dataDir.exists()) {
        dataDir.mkdirs();
      }
    }

    LOG.info("Start to write table: " + tableInfo.toString() + "-->" + dataDir.getAbsolutePath());

    File dataFile = new File(dataDir, "data");
    try {
      dataFile.createNewFile();
    } catch (IOException e1) {
    }
    CsvWriter writer = new CsvWriter(dataFile.getAbsolutePath(), inputColumnSeperator, encoding);
    try {
      for (String[] record : records) {
        writer.writeRecord(record);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    writer.close();

    LOG.info("Finished write table: " + tableInfo.toString() + "-->" + dataDir.getAbsolutePath());

    return dataDir;
  }

  /**
   * @param odps
   * @param projectName
   * @param tableName
   * @param partition
   *     if null will download all partitions
   * @param limitDownloadRecordCount
   * @param readCols
   *     if null will download all columns
   * @return
   */
  public static List<String[]> downloadTableData(Odps odps, String projectName, String tableName,
                                                 PartitionSpec partition,
                                                 int limitDownloadRecordCount, String[] readCols) {

    if (odps == null || StringUtils.isBlank(projectName) || StringUtils.isBlank(tableName)) {
      throw new IllegalArgumentException("Missing arguments: odps|projectName|tablename");
    }

    TableInfo tableInfo = TableInfo.builder().projectName(projectName).tableName(tableName)
        .partSpec(partition).build();

    validateTable(odps, tableInfo);

    LOG.info("Start to download table: '" + tableInfo.toString() + "', download mode:"
        + WareHouse.getInstance().getDownloadMode());

    List<String[]> list = new LinkedList<String[]>();
    TableMeta tableMeta = null;
    int columnCount;

    Table table = odps.tables().get(projectName, tableName);
    tableMeta = TableMeta.fromTable(table);
    TableTunnel tunnel = new TableTunnel(odps);
    String tunnelEndpoint = WareHouse.getInstance().getTunnelEndpoint();

    try {
      if (StringUtils.isNotBlank(tunnelEndpoint)) {
        tunnel.setEndpoint(tunnelEndpoint);
      }
      DownloadSession downloadSession = (partition == null) ?
          tunnel.createDownloadSession(projectName, tableName):
          tunnel.createDownloadSession(projectName, tableName, partition);
      LOG.info("Tunnel DownloadSession ID is : " + downloadSession.getId());

      RecordReader reader = downloadSession.openRecordReader(0, limitDownloadRecordCount);
      Record record = null;
      columnCount = tableMeta.getCols().length;
      while ((record = reader.read()) != null) {
        // 下载的数据不包括分区数据，分区数据在目录结构中表示，防止Map或Reduce函数处理分区数据
        String[] vals = new String[columnCount];
        for (int i = 0; i < vals.length; i++) {
          vals[i] = getColumnValueString(tableMeta, record, i);
        }
        list.add(vals);
      }
    } catch (Exception e) {
      throw new RuntimeException(LocalRunUtils.getDownloadErrorMsg(tableInfo.toString())
                                 + e.getMessage());
    }

    if (readCols == null) {
      return list;
    }

    List<Integer> indexList = LocalRunUtils.genReadColsIndexes(tableMeta, readCols);

    int indexLength = indexList == null ? columnCount : indexList.size();
    if (indexLength == columnCount) {
      return list;
    }
    List<String[]> result = new LinkedList<String[]>();
    for (String[] srcData : list) {
      String[] dstData = new String[indexLength];
      for (int i = 0; i < indexLength; ++i) {
        dstData[i] = srcData[indexList.get(i)];
        result.add(dstData);
      }
    }
    LOG.info("Finished download table: '" + tableInfo.toString() + "', download mode:"
        + WareHouse.getInstance().getDownloadMode());

    return result;

  }

  public static void downloadResource(Odps odps, String projName, String resourceName,
                                      int limitDownloadRecordCount, char inputColumnSeperator) {
    if (odps == null || StringUtils.isBlank(projName) || StringUtils.isBlank(resourceName)) {
      throw new IllegalArgumentException("Missing arguments: odps|projName|resourceName");
    }
    
    if (WareHouse.getInstance().getDownloadMode() == DownloadMode.NEVER) {
      throw new RuntimeException("Download resource '" + projName + "." + resourceName
          + "' Failed! Current download mode is:" + DownloadMode.NEVER + ".Please check parameter'"
          + Constants.LOCAL_DOWNLOAD_MODE + "'");
    }
    
    try {
      if (!odps.resources().exists(projName, resourceName)) {
        throw new RuntimeException("Download resource: " + projName + "." + resourceName
            + " Failed!Remote resource not found! Download mode:"
            + WareHouse.getInstance().getDownloadMode());
      }
    } catch (OdpsException e1) {
      throw new RuntimeException(e1);
    }

    Resource resource = odps.resources().get(projName, resourceName);
    File resFile = WareHouse.getInstance().getReourceFile(projName, resourceName);
    LOG.info("Start to download resource: " + resource.getName() + "-->"
        + resFile.getAbsolutePath() + ", type: " + resource.getType() + ",download mode:"
        + WareHouse.getInstance().getDownloadMode());

    switch (resource.getType()) {
      case FILE:
      case JAR:
      case PY:
      case ARCHIVE:
        InputStream in = null;
        FileOutputStream out = null;
        try {
          in = odps.resources().getResourceAsStream(resource.getProject(), resource.getName());
          resFile.getParentFile().mkdirs();
          out = new FileOutputStream(resFile);
          IOUtils.copy(in, out);
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (OdpsException e) {
          throw new RuntimeException(e);
        } finally {
          if (in != null) {
            try {
              in.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          if (out != null) {
            try {
              out.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

        }

        break;

      case TABLE:
        TableResource tr = new TableResource(resource);

        Table stable = tr.getSourceTable();
        String sourceProjName = stable.getProject();
        String sourceTbleName = stable.getName();
        PartitionSpec partSpec = tr.getSourceTablePartition();

        // 下载数据table到warehouse/_table__
        TableInfo tableInfo = null;
        if (partSpec == null) {
          tableInfo = TableInfo.builder().projectName(sourceProjName).tableName(sourceTbleName)
              .build();
          // 下载表所有分区数据（每张表最多limit条数）
          if (!WareHouse.getInstance().existsTable(sourceProjName, sourceTbleName)) {
            downloadTableSchemeAndData(odps, tableInfo, limitDownloadRecordCount,
                                       inputColumnSeperator);
          }
        } else {

          LinkedHashMap<String, String> partMap = PartitionUtils.convert(partSpec);
          tableInfo = TableInfo.builder().projectName(sourceProjName).tableName(sourceTbleName)
              .partSpec(partMap).build();

          // 下载分区数据
          if (!WareHouse.getInstance().existsPartition(sourceProjName, sourceTbleName, partSpec)) {
            downloadTableSchemeAndData(odps, tableInfo, limitDownloadRecordCount,
                                       inputColumnSeperator);
          }
        }

        // 创建表资源目录warehouse/__resource__/table_resource_name
        WareHouse.getInstance().createTableReourceFile(resource.getProject(), resource.getName(),
                                                       tableInfo);
        break;

      default:
        break;
    }

    LOG.info("Finished download resource: " + resource.getName() + "-->"
        + resFile.getAbsolutePath() + ", type: " + resource.getType() + ",download mode:"
        + WareHouse.getInstance().getDownloadMode());

  }

  public static TableMeta downloadTableInfo(Odps odps, TableInfo tableInfo) {
    validateTable(odps, tableInfo);
    Table table = odps.tables().get(tableInfo.getProjectName(), tableInfo.getTableName());
    return TableMeta.fromTable(table);
  }

  private static String getColumnValueString(TableMeta tableMeta, Record record, int colIndex) {
    Column col = tableMeta.getCols()[colIndex];
    return TypeConvertUtils.toString(record.get(colIndex), col.getTypeInfo(), true);
  }

  private static boolean matches(PartitionSpec spec, Map<String, String> parts) {
    for (Map.Entry<String, String> e : parts.entrySet()) {
      if (!spec.get(e.getKey()).equals(e.getValue())) {
        return false;
      }
    }
    return true;
  }

  public static void validateTable(Odps odps, TableInfo tableInfo) {
    if (tableInfo == null) {
      throw new IllegalArgumentException("Missing arugument: tableInfo");
    }
    if (WareHouse.getInstance().getDownloadMode() == DownloadMode.NEVER) {
      throw new RuntimeException("Download table schema '" + tableInfo.toString()
          + "' Failed! Current download mode is:" + DownloadMode.NEVER + ".Please check parameter '"
          + Constants.LOCAL_DOWNLOAD_MODE + "'");
    }
    try {
      if (!odps.tables().exists(tableInfo.getProjectName(), tableInfo.getTableName())) {
        throw new RuntimeException("Download table " + tableInfo
            + " Failed! Remote table not found!Download mode:"
            + WareHouse.getInstance().getDownloadMode());
      }
    } catch (OdpsException e1) {
      throw new RuntimeException(e1);
    }

    Table table = odps.tables().get(tableInfo.getProjectName(), tableInfo.getTableName());

    if (tableInfo.getPartitionSpec() != null
        && !tableInfo.getPartitionSpec().isEmpty()
        && (table.getSchema().getPartitionColumns() == null || table.getSchema()
        .getPartitionColumns().isEmpty())) {
      throw new RuntimeException("Download table " + tableInfo + "Failed! Remote table "
          + table.getProject() + "." + table.getName()
          + " is not a partitioned table,Download mode:"
          + WareHouse.getInstance().getDownloadMode());
    }

    if (table.isVirtualView()) {
      throw new RuntimeException(ExceptionCode.ODPS_0720101 + " - " + tableInfo);
    }

  }

  public static CsvReader newCsvReader(String fileName, char sep, Charset charset)
      throws FileNotFoundException {
    CsvReader reader = new CsvReader(fileName, sep, charset);
    reader.setSafetySwitch(false);
    return reader;
  }

}
