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

package com.aliyun.odps.graph.local;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.GRAPH_CONF;
import com.aliyun.odps.graph.JobConf;
import com.aliyun.odps.graph.job.JobRunner;
import com.aliyun.odps.graph.local.master.Master;
import com.aliyun.odps.graph.local.utils.LocalGraphRunUtils;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.ExceptionCode;
import com.aliyun.odps.local.common.FileSplit;
import com.aliyun.odps.local.common.JobDirecotry;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.utils.DownloadUtils;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.local.common.utils.PartitionUtils;
import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.aliyun.odps.mapred.JobStatus;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.SessionState;


public class LocalGraphJobRunner implements JobRunner {

  private static final Log LOG = LogFactory.getLog(LocalGraphJobRunner.class);
  private String jobId;
  private JobConf conf;

  private Odps odps;
  private RuntimeContext ctx;

  private List<FileSplit> inputs;
  private Map<String, TableInfo> outputs;

  private WareHouse wareHouse;
  private JobDirecotry jobDirecotry;


  private void initialize() throws IOException {
    odps = SessionState.get().getOdps();
    wareHouse = WareHouse.getInstance();
    wareHouse.init(odps, conf);

    jobId = LocalGraphRunUtils.generateLocalGraphTaskName();
    //conf.setJobName(jobId);
    conf.set(Constants.JOB_NAME, jobId);
    ctx = RuntimeContext.create(jobId, conf);
    jobDirecotry = new JobDirecotry();
    inputs = new ArrayList<FileSplit>();
  }

  @Override
  public RunningJob submit() throws OdpsException {
    try {
      initialize();
      LOG.info("run mapreduce job in local mode");
      LOG.info("job id: " + jobId);
      runJob();
    } catch (Exception e) {
      throw new OdpsException(e);
    }
    return new LocalRunningJob(jobId, JobStatus.SUCCEEDED, ctx.getCounters());
  }

  private void runJob() throws Exception {

    //jobDirecotry.writeJobConf();
    // write job configuration to temporary file job.xml
    FileOutputStream fos = new FileOutputStream(jobDirecotry.getJobFile());
    conf.writeXml(fos);
    fos.close();

    processInputs();

    processResources();

    processOutputs();

    @SuppressWarnings({"unchecked", "rawtypes"})
    Master master = new Master(LocalGraphRunUtils.getGraphJobConf(conf), ctx, inputs, outputs);

    master.run();

    moveOutputs();
    System.out.println("graph task finish");

  }

  private void processInputs() throws OdpsException, IOException {

    int maxGraphWorkers = LocalGraphRunUtils.getMaxGraphTasks();
    TableInfo[] inputTableInfos = LocalGraphRunUtils.getInputTables(conf);

    if (inputTableInfos.length > 64) {
      throw new OdpsException(ExceptionCode.ODPS_0720301);
    }
    for (TableInfo tbl : inputTableInfos) {
      processInput(tbl);
    }

    if (inputs.isEmpty()) {
      inputs.add(FileSplit.NullSplit);
    }

    checkInputsSize(maxGraphWorkers);
  }

  private void processInput(TableInfo tableInfo) throws IOException, OdpsException {
    LOG.info("Processing input: " + tableInfo);

    String projName = tableInfo.getProjectName();
    if (projName == null) {
      projName = SessionState.get().getOdps().getDefaultProject();
    }
    String tblName = tableInfo.getTableName();
    String[] readCols = tableInfo.getCols();

    // 获取指定的MR需要处理的分区
    LinkedHashMap<String, String> expectPartsHashMap = tableInfo.getPartSpec();
    PartitionSpec expectParts = null;
    if (expectPartsHashMap != null && expectPartsHashMap.size() > 0) {
      StringBuffer sb = new StringBuffer();
      for (String key : expectPartsHashMap.keySet()) {
        if (sb.length() > 0) {
          sb.append(",");
        }
        sb.append(key + "=" + expectPartsHashMap.get(key));
      }
      expectParts = new PartitionSpec(sb.toString());
    }

    // 该部分逻辑只负责下载Table Scheme和数据，后续处理和本地一样
    if (!wareHouse.existsTable(projName, tblName)) {

      DownloadUtils
          .downloadTableSchemeAndData(odps, tableInfo, wareHouse.getLimitDownloadRecordCount(),
                                      wareHouse.getInputColumnSeperator());

      if (!wareHouse.existsTable(projName, tblName)) {
        throw new OdpsException("download table from remote host failure");
      }
    }

    // ////从warehouse _scheme_读出的信息，相当与服务器端数据表的信息////
    TableMeta whTblMeta = wareHouse.getTableMeta(projName, tblName);
    Column[] whReadFields = LocalRunUtils.getInputTableFields(whTblMeta,
                                                              readCols);
    List<PartitionSpec> whParts = wareHouse.getPartitions(projName, tblName);
    // //////////////////////

    if (whParts.size() > 0) {
      // partitioned table
      for (PartitionSpec partSpec : whParts) {
        // 过滤非指定的分区
        if (!match(expectParts, partSpec)) {
          continue;
        }
        File whSrcDir = wareHouse.getPartitionDir(whTblMeta.getProjName(),
                                                  whTblMeta.getTableName(), partSpec);
        // add input split only when src dir has data file
        if (LocalRunUtils.listDataFiles(whSrcDir).size() > 0) {

          // 保持本地临时目录结构与warehouse中的目录结构一致
          File tempDataDir = jobDirecotry.getInputDir(wareHouse.getRelativePath(
              whTblMeta.getProjName(), whTblMeta.getTableName(), partSpec));
          File tempSchemeDir = jobDirecotry.getInputDir(wareHouse.getRelativePath(
              whTblMeta.getProjName(), whTblMeta.getTableName(), null));
          wareHouse.copyTable(whTblMeta.getProjName(), whTblMeta.getTableName(), partSpec,
                              readCols, tempSchemeDir, wareHouse.getLimitDownloadRecordCount(),
                              wareHouse.getInputColumnSeperator());
          for (File file : LocalRunUtils.listDataFiles(tempDataDir)) {
            inputs.add(new InputSplit(file, whReadFields, 0L, file.length(), tableInfo));
          }
        }
      }
    } else {
      // not partitioned table
      if (tableInfo.getPartSpec() != null && tableInfo.getPartSpec().size() > 0) {
        throw new IOException(ExceptionCode.ODPS_0720121 + "table "
                              + projName + "." + tblName + " is not partitioned table");
      }

      File whSrcDir = wareHouse.getTableDir(whTblMeta.getProjName(),
                                            whTblMeta.getTableName());
      if (LocalRunUtils.listDataFiles(whSrcDir).size() > 0) {
        // 保持本地临时目录结构与warehouse中的目录结构一致
        File tempDataDir = jobDirecotry.getInputDir(wareHouse.getRelativePath(
            whTblMeta.getProjName(), whTblMeta.getTableName(), null));
        File tempSchemeDir = tempDataDir;
        wareHouse.copyTable(whTblMeta.getProjName(), whTblMeta.getTableName(), null, readCols,
                            tempSchemeDir, wareHouse.getLimitDownloadRecordCount(),
                            wareHouse.getInputColumnSeperator());
        for (File file : LocalRunUtils.listDataFiles(tempDataDir)) {
          inputs.add(new InputSplit(file, whReadFields, 0L, file.length(), tableInfo));
        }
      }
    }

  }


  private void checkInputsSize(int maxGraphWorkers) throws IOException {
    if (inputs.size() > maxGraphWorkers) {
      String
          msg =
          "ODPS-0740002: Too many local-run workers : %s, must be <= %s (specified by local_run parameter 'odps.graph.local.max.workers')";
      msg = String.format(msg, inputs.size(), maxGraphWorkers);
      throw new IOException(msg);
    }
    if (inputs.size() == 0) {
      throw new IOException("ODPS-0720231: Job input is not set");
    }
  }

  private void processResources() throws IOException, OdpsException {

    File resDir = jobDirecotry.getResourceDir();

    String curProjName = SessionState.get().getOdps().getDefaultProject();
    String[] resources = conf.getStrings(GRAPH_CONF.CACHE_RESOURCES);
    int maxResouceNum = 256;

    if (resources == null || resources.length == 0) {
      return;
    }

    Set<String> names = new HashSet<String>(Arrays.asList(resources));
    LOG.info("Start to process resources: " + StringUtils.join(resources, ','));
    if (names.size() > maxResouceNum) {
      throw new IOException(ExceptionCode.ODPS_0720331
                            + " - define too many cache resources, must be <= " + maxResouceNum);
    }
    long resourceSize = 0;
    URLClassLoader loader = (URLClassLoader) Thread.currentThread()
        .getContextClassLoader();
    ArrayList<URL> cp = new ArrayList<URL>(Arrays.asList(loader.getURLs()));
    for (String name : names) {

      List<String> res = LocalRunUtils.parseResourceName(name, curProjName);

      String projName = res.get(0);
      String resName = res.get(1);

      if (!wareHouse.existsResource(projName, resName)) {
        DownloadUtils
            .downloadResource(odps, projName, resName, wareHouse.getLimitDownloadRecordCount(),
                              wareHouse.getInputColumnSeperator());
        resourceSize += new File(resDir, resName).length();

      }
      wareHouse.copyResource(projName, resName, resDir, wareHouse.getLimitDownloadRecordCount(),
                             wareHouse.getInputColumnSeperator());
      cp.add(new File(resDir, resName).toURL());
      if (resourceSize > 512 * 1024 * 1024) {
        throw new IOException(ExceptionCode.ODPS_0720071
                              + " - must be <= 512M");
      }
    }
    URLClassLoader newLoader = new URLClassLoader(cp.toArray(new URL[0]),
                                                  loader);
    Thread.currentThread().setContextClassLoader(newLoader);
    conf.setClassLoader(newLoader);
  }

  private void processOutputs() throws IOException, OdpsException {
    this.outputs = new HashMap<String, TableInfo>();
    TableInfo[] tableInfos = LocalGraphRunUtils.getOutputTables(conf);
    for (TableInfo table : tableInfos) {
      String label = table.getLabel();
      if (label.equals("__default__")) {
        label = "";
      }
      String projName = table.getProjectName();
      if (projName == null) {
        projName = SessionState.get().getOdps().getDefaultProject();
      }
      String tblName = table.getTableName();

      // FIXME: Support partition
      File tblDir = jobDirecotry.getOutputDir(table.getLabel());
      tblDir.mkdirs();
      TableMeta tblMeta = null;
      if (wareHouse.existsTable(projName, tblName)) {
        tblMeta = wareHouse.getTableMeta(projName, tblName);
      } else {
        tblMeta = DownloadUtils.downloadTableInfo(odps, table);
      }
      SchemaUtils.generateSchemaFile(tblMeta, null, tblDir);
      //conf.setOutputSchema(tblMeta.getCols(), label);
      conf.set("odps.mapred.output.schema." + label, SchemaUtils.toString(tblMeta.getCols()));
      outputs.put(label, table);
    }
  }

  private void moveOutputs() throws IOException {
    for (TableInfo table : LocalGraphRunUtils.getOutputTables(conf)) {
      String label = table.getLabel();

      String projName = table.getProjectName();
      if (projName == null) {
        projName = SessionState.get().getOdps().getDefaultProject();
      }
      String tblName = table.getTableName();
      Map<String, String> partSpec = table.getPartSpec();

      File tempTblDir = jobDirecotry.getOutputDir(table.getLabel());
      File
          whOutputDir =
          wareHouse.createPartitionDir(projName, tblName, PartitionUtils.convert(partSpec));
      if (wareHouse.existsTable(projName, tblName)) {
        LOG.info("Reload warehouse table:" + tblName);
        if (!wareHouse.isRetainTempData()) {
          LocalRunUtils.removeDataFiles(whOutputDir);
        }
        wareHouse.copyDataFiles(tempTblDir, null, whOutputDir, wareHouse.getInputColumnSeperator());
      } else {
        LOG.info("Copy output to warehouse: label=" + label + " -> "
                 + whOutputDir.getAbsolutePath());
        FileUtils.copyDirectory(tempTblDir, whOutputDir);
      }
    }
  }


  private boolean match(PartitionSpec expectedParts, PartitionSpec parts) {
    if (expectedParts == null) {
      return true;
    }
    if (parts == null || expectedParts.keys().size() > parts.keys().size()) {
      return false;
    }
    for (String key : expectedParts.keys()) {
      String expectedValue = expectedParts.get(key);
      if (expectedValue == null) {
        continue;
      }
      String value = parts.get(key);
      if (!expectedValue.equals(value)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = new JobConf(conf);
  }

}
