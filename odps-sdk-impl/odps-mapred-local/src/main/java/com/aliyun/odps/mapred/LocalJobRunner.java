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

package com.aliyun.odps.mapred;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.CounterGroup;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.DownloadMode;
import com.aliyun.odps.local.common.FileSplit;
import com.aliyun.odps.local.common.JobDirecotry;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.security.ApplicatitionType;
import com.aliyun.odps.local.common.security.SecurityClient;
import com.aliyun.odps.local.common.utils.DownloadUtils;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.local.common.utils.PartitionUtils;
import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.aliyun.odps.mapred.bridge.utils.Validator;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.local.JobCounter;
import com.aliyun.odps.mapred.local.LocalRunningJob;
import com.aliyun.odps.mapred.local.LocalTaskId;
import com.aliyun.odps.mapred.local.MRExceptionCode;
import com.aliyun.odps.mapred.local.MapDriver;
import com.aliyun.odps.mapred.local.MapOutputBuffer;
import com.aliyun.odps.mapred.local.ReduceDriver;
import com.aliyun.odps.mapred.local.StageStatic;
import com.aliyun.odps.mapred.local.utils.LocalMRUtils;
import com.aliyun.odps.mapred.local.utils.LocalValidatorFactory;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;

public class LocalJobRunner implements JobRunner {

  private List<FileSplit> inputs;
  private WareHouse wareHouse;
  private JobDirecotry jobDirecotry;
  private Counters counters;
  private Odps odps;
  private BridgeJobConf conf;
  private Map<FileSplit, TableInfo> splitToTableInfo;
  private List<StageStatic> stageStaticList;

  private static final Log LOG = LogFactory.getLog(LocalJobRunner.class);
  public static Counter EMPTY_COUNTER;

  // pipe mode
  private Pipeline pipeline;

  public void initialize() {
    odps = SessionState.get().getOdps();
    wareHouse = WareHouse.getInstance();
    wareHouse.init(odps, conf);

    LocalMRUtils.generateLocalMrTaskName();
    inputs = new ArrayList<FileSplit>();
    counters = new Counters();
    splitToTableInfo = new HashMap<FileSplit, TableInfo>();
    stageStaticList = new LinkedList<StageStatic>();
    jobDirecotry = new JobDirecotry();
    EMPTY_COUNTER = counters.findCounter(JobCounter.__EMPTY_WILL_NOT_SHOW);

    initSecurity();
  }

  private void initSecurity() {
    List<String> codeBase = new LinkedList<String>();
    // add odps-mapred-local
    String path =
        LocalJobRunner.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    path = path.substring(path.indexOf(":") + 1);
    codeBase.add(path);

    // add odps-sdk-mapred
    path = MapperBase.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    path = path.substring(path.indexOf(":") + 1);
    codeBase.add(path);

    // add odps-mapred-bridge
    path = BridgeJobConf.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    path = path.substring(path.indexOf(":") + 1);
    codeBase.add(path);


    Map<String, String> replacement = new HashMap<String, String>();
    replacement.put("\\$TEMP_HOME\\$", WareHouse.getInstance().getJobDirStr() + File.separator
        + "-");
    replacement.put("\\$WAREHOUSE_HOME\\$", WareHouse.getInstance().getWarehouseDir()
        .getAbsolutePath()
        + File.separator + "-");


    boolean isSecurityEnabled = conf.getBoolean(Constants.LOCAL_SECURITY_ENABLE, false);
    boolean isJNIEnabled = conf.getBoolean(Constants.LOCAL_SECURITY_JNI_ENABLE, false);
    String userDefinePolicy = conf.get(Constants.LOCAL_USER_DEFINE_POLICY, "");
    SecurityClient.init(ApplicatitionType.MR, codeBase, replacement, isSecurityEnabled,
        isJNIEnabled, userDefinePolicy);
  }


  @Override
  public RunningJob submit() {
    try {
      initialize();
      runJob();
      return new LocalRunningJob(wareHouse.getJobName(), JobStatus.SUCCEEDED, counters);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void runJob() throws IOException, OdpsException {
    pipeline = Pipeline.fromJobConf(conf);
    LOG.info("Run mapreduce job in local mode, Type: " + (pipeline == null ? "MR" : "MRR")
             + ", Job ID: " + wareHouse.getJobName());

    // write job configuration to temporary file job.xml
    FileOutputStream fos = new FileOutputStream(jobDirecotry.getJobFile());
    conf.writeXml(fos);
    fos.close();

    LOG.info("Start to process input tables");
    processInputs();
    LOG.info("Finished process input tables");

    LOG.info("Start to process output tables");
    processOutputs();
    LOG.info("Finished process output tables");

    LOG.info("Start to process resources");
    processResources();
    LOG.info("Finished process resources");

    LOG.info("Start to fill tableInfo");
    fillTableInfo();
    LOG.info("Finished fill tableInfo");

    LOG.info("Start to validate configuration");
    Validator validator = LocalValidatorFactory.getValidator(conf);
    validator.validate();
    LOG.info("Finished validate configuration");

    try {
      SecurityClient.open();
      if (pipeline != null) {
        handlePipeMode();
      } else {
        handleNonPipeMode();
      }
    } catch (Exception ex) {
      throw new OdpsException(ex);
    } finally {
      SecurityClient.close();
    }

    // copy output files from temporary directory to warehouse
    moveOutputs();

    // delete temporary directory if user not set odps.mapred.temp.retain=true
    try {
      if (!wareHouse.isRetainTempData()) {
        FileUtils.deleteDirectory(jobDirecotry.getJobDir());
      }
    } catch (Exception exception) {
      LOG.warn(exception.getMessage());
    }

    // print summary content
    System.err.println();
    System.err.println("Summary:");
    printInputOutput();
    printStageStatic();
    printCounters();
    System.err.println("\nOK");
  }

  private void handlePipeMode() throws IOException {

    int mapCopyNum = 0, reduceCopyNum = 0;
    if (inputs.size() > 0) {
      mapCopyNum = inputs.size();
    } else {
      // allow no input
      mapCopyNum = conf.getInt("odps.stage.mapper.num", 1);
    }
    TransformNode pipeNode = pipeline.getFirstNode();
    reduceCopyNum = computeReduceNum(mapCopyNum, pipeNode);

    LOG.info("Start to run mappers, num: " + mapCopyNum);

    TaskId taskId = new LocalTaskId("M1", 0, odps.getDefaultProject());

    StageStatic stageStatic = createStageStatic(taskId);
    stageStatic.setWorkerCount(mapCopyNum);

    MapOutputBuffer inputBuffer = new MapOutputBuffer(conf, pipeline, taskId.getTaskId(), reduceCopyNum);

    for (int mapId = 0; mapId < mapCopyNum; mapId++) {
      FileSplit split = inputs.size() > 0 ? inputs.get(mapId) : FileSplit.NullSplit;
      taskId = new LocalTaskId("M1", 0, odps.getDefaultProject());
      LOG.info(
          "Start to run mapper, TaskId: " + taskId + ", Input: " + splitToTableInfo.get(split));

      MapDriver mapDriver = new MapDriver(conf, split, taskId, inputBuffer, counters,
                                          splitToTableInfo.get(split));
      mapDriver.run();

      setInputOutputRecordCount(stageStatic);
      LOG.info(
          "Fininshed run mapper, TaskId: " + taskId + ", Input: " + splitToTableInfo.get(split));
    }
    LOG.info("Fininshed run all mappers, num: " + mapCopyNum);

    int reduceNodeCount = pipeline.getNodeNum() - 1;
    if (reduceNodeCount > 0) {
      LOG.info("Start to run reduces, num: " + reduceNodeCount);

      stageStatic.setNextTaskId("R2_1");

      int i = 0;
      for (; i < reduceNodeCount; ++i) {

        // (i+2)表示当前Reduce节点的编号,(i+1)表示前一个Map节点或Reduce节点的编号
        taskId = new LocalTaskId("R" + (i + 2) + "_" + (i + 1), 0, odps.getDefaultProject());
        LOG.info("Start to run reduce, taskId: " + taskId);

        // 前一个stage设置nextTaskId
        stageStatic.setNextTaskId("R" + (i + 2) + "_" + (i + 1));

        stageStatic = createStageStatic(taskId);
        stageStatic.setWorkerCount(reduceCopyNum);
        int nextReduceCopyNum = computeReduceNum(reduceCopyNum, pipeline.getNode(i+1));

        MapOutputBuffer outputBuffer = new MapOutputBuffer(conf, pipeline, taskId.getTaskId(), nextReduceCopyNum);

        for (int j = 0; j < reduceCopyNum; ++j) {
          taskId = new LocalTaskId("R" + (i + 2) + "_" + (i + 1), j, odps.getDefaultProject());
          ReduceDriver reduceDriver = new ReduceDriver(conf, inputBuffer, outputBuffer, taskId,
                                                       counters, j);
          reduceDriver.run();

          setInputOutputRecordCount(stageStatic);
        }

        // 前一个节点的输出作为下一个节点的输入
        inputBuffer = outputBuffer;
        reduceCopyNum = nextReduceCopyNum;

        LOG.info("Finished run reduce, taskId: " + taskId);
      }

      // 最后一个stage设置nextTaskId
      stageStatic.setNextTaskId("R" + (i + 1) + "_" + i + "FS_9");

      LOG.info("Fininshed run all reduces, num: " + reduceNodeCount);

    } else {
      stageStatic.setNextTaskId("M1");
      LOG.info("This is a MapOnly job");
    }

  }

  private void handleNonPipeMode() throws IOException {
    int mapCopyNum = 0, reduceCopyNum = 0;
    if (inputs.size() > 0) {
      mapCopyNum = inputs.size();
    } else {
      // allow no input
      mapCopyNum = conf.getInt("odps.stage.mapper.num", 1);
    }
    reduceCopyNum = computeReduceNum(mapCopyNum, null);
    MapOutputBuffer buffer = new MapOutputBuffer(conf, reduceCopyNum);

    LOG.info("Start to run mappers, num: " + mapCopyNum);

    TaskId taskId = new LocalTaskId("M1", 0, odps.getDefaultProject());
    StageStatic stageStatic = createStageStatic(taskId);
    stageStatic.setWorkerCount(mapCopyNum);

    for (int mapId = 0; mapId < mapCopyNum; mapId++) {
      FileSplit split = inputs.size() > 0 ? inputs.get(mapId) : FileSplit.NullSplit;
      taskId = new TaskId("M", mapId + 1);
      LOG.info(
          "Start to run mapper, TaskId: " + taskId + ", Input: " + splitToTableInfo.get(split));

      MapDriver mapDriver = new MapDriver(conf, split, taskId, buffer, counters,
                                          splitToTableInfo.get(split));
      mapDriver.run();

      setInputOutputRecordCount(stageStatic);
      LOG.info(
          "Fininshed run mapper, TaskId: " + taskId + ", Input: " + splitToTableInfo.get(split));
    }

    LOG.info("Fininshed run all mappers, num: " + mapCopyNum);

    if (reduceCopyNum > 0) {
      LOG.info("Start to run reduces, num: " + reduceCopyNum);

      taskId = new LocalTaskId("R2_1", 0, odps.getDefaultProject());
      // 前一个stage设置nextTaskId
      stageStatic.setNextTaskId("R2_1");

      stageStatic = createStageStatic(taskId);
      stageStatic.setWorkerCount(reduceCopyNum);

      for (int reduceId = 0; reduceId < reduceCopyNum; ++reduceId) {

        taskId = new TaskId("R", reduceId);
        LOG.info("Start to run reduce, taskId: " + taskId);

        ReduceDriver
            reduceDriver =
            new ReduceDriver(conf, buffer, null, taskId, counters, reduceId);
        reduceDriver.run();

        setInputOutputRecordCount(stageStatic);
        LOG.info("Finished run reduce, taskId: " + taskId);
      }

      stageStatic.setNextTaskId("R2_1FS_9");

      LOG.info("Fininshed run all reduces, num: " + reduceCopyNum);

    } else {
      stageStatic.setNextTaskId("M1");
      LOG.info("This is a MapOnly job");
    }

  }

  private StageStatic createStageStatic(TaskId taskId) {
    StageStatic stageStatic = new StageStatic();
    stageStaticList.add(stageStatic);
    stageStatic.setTaskId(taskId.toString());
    return stageStatic;
  }

  private void setInputOutputRecordCount(StageStatic stageStatic) {
    stageStatic.setInputRecordCount(counters.findCounter(JobCounter.__EMPTY_INPUT_RECORD_COUNT)
                                        .getValue());
    stageStatic.setOutputRecordCount(counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT)
                                         .getValue());
    counters.findCounter(JobCounter.__EMPTY_INPUT_RECORD_COUNT).setValue(0);
    counters.findCounter(JobCounter.__EMPTY_OUTPUT_RECORD_COUNT).setValue(0);
  }

  private void processInput(TableInfo tableInfo) throws IOException, OdpsException {
    if (tableInfo == null || StringUtils.isBlank(tableInfo.getTableName())) {
      throw new RuntimeException("Invalid TableInfo: " + tableInfo);
    }

    if (StringUtils.isEmpty(tableInfo.getProjectName())) {
      tableInfo.setProjectName(wareHouse.getOdps().getDefaultProject());
    }

    String[] readCols = tableInfo.getCols();

    // 获取指定的MR需要处理的分区
    PartitionSpec expectParts = tableInfo.getPartitionSpec();

    // 该部分逻辑只负责下载Table Scheme和数据，后续处理和本地一样
    if (!wareHouse
        .existsPartition(tableInfo.getProjectName(), tableInfo.getTableName(), expectParts)
        ||wareHouse.getDownloadMode()==DownloadMode.ALWAYS) {

      DownloadUtils
          .downloadTableSchemeAndData(odps, tableInfo, wareHouse.getLimitDownloadRecordCount(),
                                      wareHouse.getInputColumnSeperator());

      if (!wareHouse
          .existsPartition(tableInfo.getProjectName(), tableInfo.getTableName(), expectParts)) {
        throw new RuntimeException(LocalRunUtils.getDownloadErrorMsg(tableInfo.toString()));
      }
    }

    // ////从warehouse _scheme_读出的信息，相当于服务器端数据表的信息////
    TableMeta
        whTblMeta =
        wareHouse.getTableMeta(tableInfo.getProjectName(), tableInfo.getTableName());
    Column[] whReadFields = LocalRunUtils.getInputTableFields(whTblMeta, readCols);
    List<PartitionSpec>
        whParts =
        wareHouse.getPartitions(tableInfo.getProjectName(), tableInfo.getTableName());

    if (whParts.size() > 0) {
      // partitioned table
      for (PartitionSpec partSpec : whParts) {
        // 过滤非指定的分区
        if (!PartitionUtils.match(expectParts, partSpec)) {
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
            FileSplit split = new FileSplit(file, whReadFields, 0L, file.length());
            splitToTableInfo.put(split, tableInfo);
            inputs.add(split);
          }
        }
      }
    } else {
      // not partitioned table
      if (tableInfo.getPartSpec() != null && tableInfo.getPartSpec().size() > 0) {
        throw new IOException(
            MRExceptionCode.ODPS_0720121 + "table " + tableInfo.getProjectName() + "." + tableInfo
                .getTableName()
            + " is not partitioned table");
      }

      File whSrcDir = wareHouse.getTableDir(whTblMeta.getProjName(), whTblMeta.getTableName());
      if (LocalRunUtils.listDataFiles(whSrcDir).size() > 0) {

        // 保持本地临时目录结构与warehouse中的目录结构一致
        File tempDataDir = jobDirecotry.getInputDir(wareHouse.getRelativePath(
            whTblMeta.getProjName(), whTblMeta.getTableName(), null));
        File tempSchemeDir = tempDataDir;
        wareHouse.copyTable(whTblMeta.getProjName(), whTblMeta.getTableName(), null, readCols,
                            tempSchemeDir, wareHouse.getLimitDownloadRecordCount(),
                            wareHouse.getInputColumnSeperator());
        for (File file : LocalRunUtils.listDataFiles(tempDataDir)) {
          FileSplit split = new FileSplit(file, whReadFields, 0L, file.length());
          splitToTableInfo.put(split, tableInfo);
          inputs.add(split);
        }
      }
    }

  }

  private void processInputs() throws IOException, OdpsException {

    // UserConfChecker.checkMapTasks(conf);

    TableInfo[] inputTableInfos = InputUtils.getTables(conf);

    if (inputTableInfos == null) {
      LOG.debug("No input tables to process");
      return;
    }

    for (TableInfo tableInfo : inputTableInfos) {
      LOG.debug("Start to process input table: " + tableInfo);
      processInput(tableInfo);
      LOG.debug("Finished process input table: " + tableInfo);

    }

    if (inputs.isEmpty()) {
      inputs.add(FileSplit.NullSplit);
    }
  }

  @SuppressWarnings("deprecation")
  private void processResources() throws IOException, OdpsException {
    String[] resources = conf.getResources();

    if (resources == null || resources.length == 0) {
      LOG.debug("No resources to process");
      return;
    }

    Set<String> names = new HashSet<String>(Arrays.asList(resources));
    LOG.info("Start to process resources: " + StringUtils.join(resources, ','));

    URLClassLoader loader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    ArrayList<URL> cp = new ArrayList<URL>(Arrays.asList(loader.getURLs()));
    String curProjName = wareHouse.getOdps().getDefaultProject();
    File resDir = jobDirecotry.getResourceDir();
    for (String name : names) {

      List<String> res = LocalRunUtils.parseResourceName(name, curProjName);

      String projName = res.get(0);
      String resName = res.get(1);

      if (!wareHouse.existsResource(projName, resName)
          || wareHouse.getDownloadMode() == DownloadMode.ALWAYS) {
        DownloadUtils.downloadResource(odps, projName, resName,
            wareHouse.getLimitDownloadRecordCount(), wareHouse.getInputColumnSeperator());
      }
      wareHouse.copyResource(projName, resName, resDir, wareHouse.getLimitDownloadRecordCount(),
                             wareHouse.getInputColumnSeperator());
      cp.add(new File(resDir, resName).toURI().toURL());
    }
    URLClassLoader newLoader = new URLClassLoader(cp.toArray(new URL[0]), loader);
    Thread.currentThread().setContextClassLoader(newLoader);
    conf.setClassLoader(newLoader);
  }

  private void processOutputs() throws IOException {
    TableInfo[] outputs = OutputUtils.getTables(conf);

    if (outputs == null || outputs.length == 0) {
      LOG.debug("No output tables to process");
      return;
    }

    for (TableInfo tableInfo : outputs) {

      if (StringUtils.isBlank(tableInfo.getProjectName())) {
        tableInfo.setProjectName(wareHouse.getOdps().getDefaultProject());
      }

      // FIXME: Support partition
      File tableDirInJobDir = jobDirecotry.getOutputDir(tableInfo.getLabel());
      tableDirInJobDir.mkdirs();
      TableMeta tblMeta = null;
      if (wareHouse.existsTable(tableInfo.getProjectName(), tableInfo.getTableName())
          && wareHouse.getDownloadMode() != DownloadMode.ALWAYS) {
        tblMeta = wareHouse.getTableMeta(tableInfo.getProjectName(), tableInfo.getTableName());
      } else {
        tblMeta = DownloadUtils.downloadTableInfo(odps, tableInfo);

        //generate output table schema in warehouse
        File
            tableDirInWarehouse =
            wareHouse.getTableDir(tableInfo.getProjectName(), tableInfo.getTableName());
        tableDirInWarehouse.mkdirs();
        SchemaUtils.generateSchemaFile(tblMeta, null, tableDirInWarehouse);

      }
      SchemaUtils.generateSchemaFile(tblMeta, null, tableDirInJobDir);
      conf.setOutputSchema(tblMeta.getCols(), tableInfo.getLabel());
    }
  }

  private void fillTableInfo() throws IOException {

    TableInfo[] infos = new TableInfo[splitToTableInfo.size()];
    splitToTableInfo.values().toArray(infos);
    String project = wareHouse.getOdps().getDefaultProject();

    for (FileSplit key : splitToTableInfo.keySet()) {
      TableInfo info = splitToTableInfo.get(key);
      if (info.getProjectName() == null) {
        info.setProjectName(project);
      }

      Column[] schema = wareHouse.getTableMeta(info.getProjectName(), info.getTableName())
          .getCols();
      if (info.getCols() == null) {
        conf.setInputSchema(info, schema);
        info.setCols(SchemaUtils.getColumnNames(schema));
      } else {
        Column[] columns = new Column[info.getCols().length];
        for (int k = 0; k < info.getCols().length; k++) {
          String colName = info.getCols()[k];
          for (Column c : schema) {
            if (c.getName().equalsIgnoreCase(colName)) {
              columns[k] = c;
              break;
            }
          }
        }
        conf.setInputSchema(info, columns);
      }
    }

    //fill input table
    infos = InputUtils.getTables(conf);
    if (infos != null) {
      boolean changed = false;
      for (int i = 0; i < infos.length; i++) {
        TableInfo info = infos[i];
        if (info.getProjectName() == null) {
          changed = true;
          info.setProjectName(project);
        }

        Column[] schema =
            wareHouse.getTableMeta(info.getProjectName(), info.getTableName()).getCols();
        if (info.getCols() == null) {
          changed = true;
          conf.setInputSchema(info, schema);
          info.setCols(SchemaUtils.getColumnNames(schema));
        } else {
          Column[] columns = new Column[info.getCols().length];
          for (int k = 0; k < info.getCols().length; k++) {
            String colName = info.getCols()[k];
            for (Column c : schema) {
              if (c.getName().equalsIgnoreCase(colName)) {
                columns[k] = c;
                break;
              }
            }
          }
          conf.setInputSchema(info, columns);
        }
        infos[i] = info;
      }
      if (changed) {
        InputUtils.setTables(infos, conf);
      }
    }

    // Expand output columns.
    infos = OutputUtils.getTables(conf);
    if (infos == null) {
      conf.setOutputSchema(new Column[]{new Column("nil", OdpsType.STRING)},
                           TableInfo.DEFAULT_LABEL);
    } else {
      for (TableInfo info : infos) {
        if (info.getProjectName() == null) {
          info.setProjectName(project);
        }
        Column[] schema = wareHouse.getTableMeta(info.getProjectName(), info.getTableName())
            .getCols();
        info.setCols(SchemaUtils.getColumnNames(schema));
        conf.setOutputSchema(schema, info.getLabel());
      }
      OutputUtils.setTables(infos, conf);
    }

  }

  private void moveOutputs() throws IOException {
    TableInfo[] output = OutputUtils.getTables(conf);
    if (output == null) {
      return;
    }
    for (TableInfo table : output) {
      String label = table.getLabel();

      String projName = table.getProjectName();
      if (projName == null) {
        projName = wareHouse.getOdps().getDefaultProject();
      }
      String tblName = table.getTableName();
      Map<String, String> partSpec = table.getPartSpec();

      File tempTblDir = jobDirecotry.getOutputDir(table.getLabel());
      File
          whOutputDir =
          wareHouse.createPartitionDir(projName, tblName, PartitionUtils.convert(partSpec));
      if (wareHouse.existsTable(projName, tblName)) {
        LOG.info("Reload warehouse table:" + tblName);
        LocalRunUtils.removeDataFiles(whOutputDir);
        wareHouse.copyDataFiles(tempTblDir, null, whOutputDir, wareHouse.getInputColumnSeperator());
      } else {
        LOG.info("Copy output to warehouse: label=" + label + " -> "
                 + whOutputDir.getAbsolutePath());
        File whOutputTableDir = wareHouse.getTableDir(projName, tblName);
        // copy schema file
        FileUtils.copyDirectory(tempTblDir, whOutputTableDir, new FileFilter() {
          @Override
          public boolean accept(File pathname) {
            String filename = pathname.getName();
            if (filename.equals("__schema__")) {
              return true;
            }
            return false;
          }
        });
        // copy data files
        FileUtils.copyDirectory(tempTblDir, whOutputDir, new FileFilter() {
          @Override
          public boolean accept(File pathname) {
            String filename = pathname.getName();
            if (filename.equals("__schema__")) {
              return false;
            }
            return true;
          }
        });
      }
    }
  }

  private int computeReduceNum(int mapNum, TransformNode pipeNode) throws IOException {
    int reduceNum = 1;
    if (pipeNode != null) {
      if (pipeNode.getNextNode() != null) {
        reduceNum = pipeNode.getNextNode().getNumTasks();
      } else {
        reduceNum = pipeNode.getNumTasks();
      }
    } else if (wareHouse.caintainsKey("odps.stage.reducer.num")) {
      reduceNum = conf.getNumReduceTasks();
    } else {
      reduceNum = Math.max(1, mapNum / 4);
    }
    if (reduceNum < 0) {
      throw new IOException(MRExceptionCode.ODPS_0720251 + " - reduce num cann't be less than 0");
    } else if (reduceNum != conf.getNumReduceTasks()) {
      LOG.info("change reduce num from " + conf.getNumReduceTasks() + " to " + reduceNum);
    }
    conf.setNumReduceTasks(reduceNum);
    return reduceNum;
  }

  private void printInputOutput() {
    StringBuffer sb = new StringBuffer();

    System.err.println("Inputs:");
    TableInfo[] tableInfos = InputUtils.getTables(conf);
    if (tableInfos != null) {
      for (TableInfo tableInfo : tableInfos) {
        if (sb.length() > 0) {
          sb.append(",");
        }
        if (tableInfo.getProjectName() != null) {
          sb.append(tableInfo.getProjectName());
        } else {
          sb.append(odps.getDefaultProject());
        }
        sb.append(".");
        sb.append(tableInfo.getTableName());
        String parts = tableInfo.getPartPath();
        if (parts != null && !parts.trim().isEmpty()) {
          sb.append("/");
          if (parts.endsWith("/")) {
            parts = parts.substring(0, parts.length() - 1);
          }
          sb.append(parts);
        }

      }
    }

    if (sb.length() > 0) {
      System.err.println("\t" + sb.toString());
      sb.delete(0, sb.length());
    }

    System.err.println("Outputs:");
    tableInfos = OutputUtils.getTables(conf);
    if (tableInfos != null) {
      for (TableInfo tableInfo : tableInfos) {
        if (sb.length() > 0) {
          sb.append(",");
        }
        if (tableInfo.getProjectName() != null) {
          sb.append(tableInfo.getProjectName());
        } else {
          sb.append(odps.getDefaultProject());
        }
        sb.append(".");
        sb.append(tableInfo.getTableName());
        String parts = tableInfo.getPartPath();
        if (parts != null && !parts.trim().isEmpty()) {
          sb.append("/");
          if (parts.endsWith("/")) {
            parts = parts.substring(0, parts.length() - 1);
          }
          sb.append(parts);
        }

      }
    }
    if (sb.length() > 0) {
      System.err.println("\t" + sb.toString());
      sb.delete(0, sb.length());
    }

  }

  private void printStageStatic() {
    StringBuilder sb = new StringBuilder();
    for (StageStatic item : stageStaticList) {
      sb.append("\n" + item.getTaskId());
      sb.append("\n\tWorker Count: " + item.getWorkerCount());

      sb.append("\n\tInput Records: ");
      sb.append("\n\t\tinput: ");
      sb.append(item.getTotalInputRecords());
      sb.append(" (min: ");
      sb.append(item.getMinInputRecords());
      sb.append(", max: ");
      sb.append(item.getMaxInputRecords());
      sb.append(", avg: ");
      sb.append(item.getAvgInputRecords());
      sb.append(")");

      sb.append("\n\tOutput Records: ");
      sb.append("\n\t\t");
      sb.append(item.getNextTaskId());
      sb.append(": ");
      sb.append(item.getTotalOutputRecords());
      sb.append(" (min: ");
      sb.append(item.getMinOutputRecords());
      sb.append(", max: ");
      sb.append(item.getMaxOutputRecords());
      sb.append(", avg: ");
      sb.append(item.getAvgOutputRecords());
      sb.append(")");
    }

    System.err.println(sb.toString());
  }

  private void printCounters() {
    int totalCount = 0;
    int frameWorkCounterCount = 0;
    int jobCounterCount = 0;
    int userCounterCount = 0;
    for (CounterGroup group : counters) {
      for (Counter counter : group) {
        if (counter.getDisplayName().startsWith("__EMPTY_")) {
          continue;
        } else if (group.getDisplayName().equals(JobCounter.class.getName())) {
          ++frameWorkCounterCount;
        } else if (group.getDisplayName()
            .equals("com.aliyun.odps.mapred.local.Counter.JobCounter")) {
          ++jobCounterCount;
        } else {
          ++userCounterCount;
        }

        ++totalCount;

      }
    }

    StringBuilder sb = new StringBuilder("Counters: " + totalCount);
    sb.append("\n\tMap-Reduce Framework: " + frameWorkCounterCount);
    for (CounterGroup group : counters) {
      if (!group.getDisplayName().equals(JobCounter.class.getName())) {
        continue;
      }
      for (Counter counter : group) {
        if (counter.getDisplayName().startsWith("__EMPTY_")) {
          continue;
        }
        sb.append("\n\t\t" + counter.getDisplayName() + "=" + counter.getValue());
      }
    }

    // sb.append("\n\tJob Counters: " + jobCounterCount);
    // for (CounterGroup group : counters) {
    // if
    // (!group.getDisplayName().equals("com.aliyun.odps.mapred.local.Counter.JobCounter"))
    // {
    // continue;
    // }
    // for (Counter counter : group) {
    // if
    // (counter.getDisplayName().equals(JobCounter.__EMPTY_WILL_NOT_SHOW.toString()))
    // continue;
    // sb.append("\n\t\t" + counter.getDisplayName() + "=" +
    // counter.getValue());
    // }
    // }

    sb.append("\n\tUser Defined Counters: " + userCounterCount);
    for (CounterGroup group : counters) {
      if (group.getDisplayName().equals(JobCounter.class.getName())
          || group.getDisplayName().equals("com.aliyun.odps.mapred.local.Counter.JobCounter")) {
        continue;
      }
      sb.append("\n\t\t" + group.getDisplayName());
      for (Counter counter : group) {
        if (counter.getDisplayName().equals(JobCounter.__EMPTY_WILL_NOT_SHOW.toString())) {
          continue;
        }
        sb.append("\n\t\t\t" + counter.getDisplayName() + "=" + counter.getValue());
      }
    }
    System.err.println(sb.toString().toLowerCase());
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = new BridgeJobConf(conf);
  }

}
