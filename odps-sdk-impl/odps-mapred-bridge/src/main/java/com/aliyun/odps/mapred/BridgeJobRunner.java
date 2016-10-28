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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.parser.Feature;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.conf.Configured;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.bridge.BridgeRunningJob;
import com.aliyun.odps.mapred.bridge.ErrorCode;
import com.aliyun.odps.mapred.bridge.MetaExplorer;
import com.aliyun.odps.mapred.bridge.MetaExplorerImpl;
import com.aliyun.odps.mapred.bridge.utils.Validator;
import com.aliyun.odps.mapred.bridge.utils.ValidatorFactory;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public abstract class BridgeJobRunner extends Configured implements JobRunner, EventListener {

  protected static final Log LOG = LogFactory.getLog(BridgeJobRunner.class);

  private static final String TEMP_RESOURCE_PREFIX = "file:";

  protected BridgeJobConf job;
  protected String jobId;
  protected boolean isClean;
  protected String taskName;

  protected Map<String, String> aliasToTempResource = new HashMap<String, String>();

  protected MetaExplorer metaExplorer;

  /**
   * Create jar with jobconf.
   *
   * @return
   * @throws OdpsException
   */
  private ByteArrayOutputStream createJarArchive() throws OdpsException {
    try {
      ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
      // Open archive file
      JarOutputStream out = new JarOutputStream(archiveOut, new Manifest());

      ByteArrayOutputStream jobOut = new ByteArrayOutputStream();
      job.writeXml(jobOut);
      // Add jobconf entry
      JarEntry jobconfEntry = new JarEntry("jobconf.xml");
      out.putNextEntry(jobconfEntry);
      out.write(jobOut.toByteArray());

      out.close();
      return archiveOut;
    } catch (IOException ex) {
      throw new OdpsException(ErrorCode.UNEXPECTED.toString(), ex);
    }
  }

  private void applyFrameworkResources() throws OdpsException {
    // Add framework jars

    String padding = "-" + jobId;

    Set<String> added = new HashSet<String>();

    applyFrameworkResource(Odps.class, "odps-sdk-core.jar", padding, added);
    applyFrameworkResource(Mapper.class, "odps-sdk-mapred.jar", padding, added);
    applyFrameworkResource(BridgeJobRunner.class, "odps-mapred-bridge.jar",
        padding, added);
    applyFrameworkResource(JSON.class, "fastjson.jar",
                           padding, added);
  }

  private void applyFrameworkResource(Class<?> clz, String alias,
      String padding, Set<String> added) throws OdpsException {
    String jarFilePath;
    try {
      jarFilePath = new File(clz.getProtectionDomain().getCodeSource().getLocation().toURI()).getAbsolutePath();
    } catch (URISyntaxException ex) {
      throw new OdpsException(ex);
    }
    if (added.contains(jarFilePath)) {
      return;
    }
    int trycount = 0;
    while (true) {
      try {
        aliasToTempResource.put(alias, metaExplorer
            .addFileResourceWithRetry(jarFilePath, Resource.Type.JAR, padding,
                true));
        added.add(jarFilePath);
        return;
      } catch (Exception ex) {
        trycount++;
        if (trycount >= 3) {
          throw new OdpsException(ex);
        }
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
        }
      }
    }

  }

  @SuppressWarnings("unchecked")
  protected void setUp() throws OdpsException {
    // Prepare additional config parameters

    // merge streaming job alias resources if exist
    if (job.get("stream.temp.resource.alias") != null) {
      String aliasJson = job.get("stream.temp.resource.alias");
      try {
        aliasToTempResource.putAll((Map<String, String>) JSON.parseObject(
            aliasJson, Map.class, Feature.OrderedField));
      } catch (JSONException e) {
        throw new OdpsException("parse stream temp resource alias json failed!", e);
      }
    }
    // for user defined partitioner, estimate reduce number if not set
    boolean isEstimateReduceNum =
        (job.getPartitionerClass() != null) && (job.get("odps.stage.reducer.num") == null);
    long inputSize = 0;
    // Expand input columns if applicable.
    TableInfo[] infos = InputUtils.getTables(job);
    // for multi inputs not allow inner output in mapper
    if (infos != null && infos.length > 1) {
      job.setMapperInnerOutputEnable(false);
    }
    String project = metaExplorer.getDefaultProject();
    boolean changed = false;
    if (infos != null) {
      for (int i = 0; i < infos.length; i++) {
        TableInfo info = infos[i];
        if (info.getProjectName() == null) {
          changed = true;
          info.setProjectName(project);
        }

        Table tbl = metaExplorer.getTable(info.getProjectName(), info.getTableName());
        List<Column> schema = tbl.getSchema().getColumns();
        String[] inputCols = getInputColumnsFromCommandSettings(job, info);
        if (inputCols.length == 0 && info.getCols() == null) {
          changed = true;
          Column[] columns = schema.toArray(new Column[schema.size()]);
          job.setInputSchema(info, columns);
          info.setCols(SchemaUtils.getNames(columns));
        } else {
          if (inputCols.length == 0) {
            inputCols = info.getCols();
          }
          Column[] columns = new Column[inputCols.length];
          for (int k = 0; k < inputCols.length; k++) {
            String colName = inputCols[k];
            for (Column c : schema) {
              if (c.getName().equalsIgnoreCase(colName)) {
                columns[k] = c;
                break;
              }
            }
          }
          job.setInputSchema(info, columns);
        }
        if (isEstimateReduceNum) {
          PartitionSpec part = info.getPartitionSpec();
          if (!part.isEmpty()) {
            // for partition table input
            inputSize += tbl.getPartition(part).getSize();
          } else {
            inputSize += tbl.getSize();
          }
        }
      }
    }
    if (changed) {
      InputUtils.setTables(infos, job);
    }
    if (isEstimateReduceNum) {
      job.setNumReduceTasks(estimateReduceNum(inputSize, job));
    }

    //add project information for volume if necessary
    changed = false;
    VolumeInfo[] volumeInfos = InputUtils.getVolumes(job);
    if (volumeInfos != null) {
      for (VolumeInfo volume : volumeInfos) {
        if (volume.getProjectName() == null) {
          changed = true;
          volume.setProjectName(project);
        }
      }
    }
    if (changed) {
      InputUtils.setVolumes(volumeInfos, job);
    }
    changed = false;
    volumeInfos = OutputUtils.getVolumes(job);
    if (volumeInfos != null) {
      for (VolumeInfo volume : volumeInfos) {
        if (volume.getProjectName() == null) {
          changed = true;
          volume.setProjectName(project);
        }
      }
    }
    if (changed) {
      OutputUtils.setVolumes(volumeInfos, job);
    }

    // Expand output columns.
    infos = OutputUtils.getTables(job);
    if (infos == null) {
      job.setOutputSchema(new Column[]{new Column("nil", OdpsType.STRING)},
                          TableInfo.DEFAULT_LABEL);
    } else {
      for (TableInfo info : infos) {
        if (info.getProjectName() == null) {
          info.setProjectName(project);
        }
        List<Column> schema = metaExplorer.getTable(info.getProjectName(), info.getTableName())
            .getSchema().getColumns();
        Column[] schemaArray = schema.toArray(new Column[schema.size()]);
        info.setCols(SchemaUtils.getNames(schemaArray));
        job.setOutputSchema(schemaArray, info.getLabel());
      }
      OutputUtils.setTables(infos, job);
    }

    processTempResources();

    // Adding jobconf jar.
    ByteArrayOutputStream jarOut = null;
    try {
      jarOut = createJarArchive();
      jarOut.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String resName = metaExplorer.addTempResourceWithRetry(
        new ByteArrayInputStream(jarOut.toByteArray()), jobId + ".jar", Resource.Type.JAR);
    aliasToTempResource.put("jobconf.jar", resName);

    applyFrameworkResources();

    List<String> totalRes = new ArrayList<String>();
    String[] resources = job.getResources();
    if (resources != null) {
      Collections.addAll(totalRes, resources);
    }
    totalRes.addAll(aliasToTempResource.keySet());
    job.setResources(StringUtils.join(totalRes, ","));
  }

  private String[] getInputColumnsFromCommandSettings(BridgeJobConf job, TableInfo info) {
    String[] columns = new String[0];
    String fullTableName = info.getProjectName() + "." + info.getTableName();
    String colsSetting = job.get("odps.mapred.input.columns." + fullTableName);
    if (colsSetting != null) {
      columns = colsSetting.split(",");
    }
    return columns;
  }

  private int estimateReduceNum(long inputSize, BridgeJobConf job) {
    long splitSize = job.getSplitSize() * 1024 * 1024; // bytes
    int numMapTasks = 1;
    if (inputSize > 0) {
      numMapTasks = (int)(inputSize / splitSize) + 1;
    } else {
      numMapTasks = job.getNumMapTasks();
    }

    return (int)(numMapTasks / 3) + 1;
  }

  /**
   * Register temporary resources.
   */
  private void processTempResources() throws OdpsException {
    String[] res = job.getResources();
    if (res == null) {
      return;
    }
    StringBuilder sb = new StringBuilder();
    for (String r : res) {
      if (r.toLowerCase().startsWith(TEMP_RESOURCE_PREFIX)) {
        Resource.Type type = r.endsWith(".jar") ? Resource.Type.JAR : Resource.Type.FILE;
        URL url;
        try {
          url = new URL(r);
        } catch (MalformedURLException e) {
          throw new OdpsException(e);
        }
        aliasToTempResource.put(FilenameUtils.getName(url.getPath()),
                                metaExplorer
                                    .addFileResourceWithRetry(url.getPath(), type, "_" + jobId,
                                                              true));
      } else {
        if (sb.length() > 0) {
          sb.append(',');
        }
        sb.append(r);
      }
    }
    job.setResources(sb.toString());
  }

  protected void tearDown() throws OdpsException {
    // Remove resources
    for (String resource : aliasToTempResource.values()) {
      metaExplorer.deleteResource(resource);
    }
    isClean = true;
  }

  abstract protected Instance submitInternal() throws OdpsException;

  @Override
  public RunningJob submit() throws OdpsException {
    // JobConf field should be initialized here because ReflectionUtils is
    // designed to create object
    // and then set JobConf.
    job = new BridgeJobConf(getConf());
    metaExplorer = getMetaExplorer();
    jobId = System.currentTimeMillis() + RandomStringUtils.randomNumeric(8);
    // Set a prefix to job name because odps only accept job name whose pattern
    // is '([a-z]|[A-Z]){1,}([a-z]|[A-Z]|[\d]|_)*' ...
    taskName = "MRonSQL_" + jobId;

    Validator validator = ValidatorFactory.getValidator(job, metaExplorer);
    validator.validate();

    setUp();

    Instance instance = submitInternal();
    BridgeRunningJob runningJob = new BridgeRunningJob(instance, taskName, this);
    if (SessionState.get().isCostMode()) {
      runningJob.setIsCostMode(true);
    }
    return runningJob;
  }

  protected MetaExplorer getMetaExplorer() {
    return new MetaExplorerImpl(SessionState.get().getOdps());
  }

  @Override
  public void onComplete() {
    try {
      if (!isClean) {
        tearDown();
      }
    } catch (OdpsException e) {
      // Silently swallow it.
    }
  }

}
