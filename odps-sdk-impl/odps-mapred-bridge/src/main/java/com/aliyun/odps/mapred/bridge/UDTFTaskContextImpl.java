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

package com.aliyun.odps.mapred.bridge;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import org.apache.commons.lang.ArrayUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.bridge.utils.ResourceUtils;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;
import com.aliyun.odps.udf.ExecutionContext;

/**
 *
 */
public abstract class UDTFTaskContextImpl implements TaskContext {

  protected BridgeJobConf conf;
  protected TaskId taskId;
  protected ExecutionContext ctx;

  // pipeline mode
  protected Pipeline pipeline;
  protected boolean pipeMode; // use pipeline mapreduce
  protected int pipeIndex = -1;
  protected TransformNode pipeNode;
  
  protected int reducerNum = 0;

  // for inner output
  protected boolean innerOutput;
  

  public UDTFTaskContextImpl(BridgeJobConf conf) {
    this.conf = conf;
    this.pipeline = Pipeline.fromJobConf(conf);
    if (this.pipeline != null) {
      this.pipeMode = true;
    }
    innerOutput = isMapper() ?
        (conf.getInnerOutputEnable() && conf.getMapperInnerOutputEnable()) :
        conf.getInnerOutputEnable();

    initOutputSchema();
  }

  boolean isMapper() {
    return false;
  }

  @Override
  public Class<? extends Reducer> getCombinerClass() {
    return conf.getCombinerClass();
  }

  @Override
  public Column[] getMapOutputKeySchema() {
    return conf.getMapOutputKeySchema();
  }

  public Column[] getMapOutputKeySchema4Pipeline(int nodeId) {
    return conf.getMapOutputKeySchema4Pileline(nodeId);
  }

  public Column[] getMapOutputValueSchema4Pipeline(int nodeId) {
    return conf.getMapOutputValueSchema4Pileline(nodeId);
  }

  @Override
  public Column[] getMapOutputValueSchema() {
    return conf.getMapOutputValueSchema();
  }

  @Override
  public Class<? extends Mapper> getMapperClass() {
    return conf.getMapperClass();
  }

  @Override
  public int getNumReduceTasks() {
    return reducerNum;
  }

  @Override
  public Class<? extends Reducer> getReducerClass() {
    return conf.getReducerClass();
  }

  @Override
  public Record createOutputRecord() throws IOException {
    return createOutputRecord(TableInfo.DEFAULT_LABEL);
  }

  @Override
  public Record createOutputRecord(String label) throws IOException {
    if (!hasLabel(label)) {
      throw new IOException(ErrorCode.NO_SUCH_LABEL.toString() + " " + label);
    }
    return new WritableRecord(conf.getOutputSchema(label));
  }

  @Override
  public Counter getCounter(Enum<?> key) {
    return ctx.getCounter(key);
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return ctx.getCounter(groupName, counterName);
  }

  @Override
  public TaskId getTaskID() {
    return taskId;
  }

  @Override
  public void progress() {
    ctx.claimAlive();
  }

  @Override
  public BufferedInputStream readResourceFileAsStream(String name) throws IOException {
    return ResourceUtils.readResourceFileAsStream(name);
  }
  
  @Override
  public Iterable<BufferedInputStream> readResourceArchiveAsStream(String resourceName) throws IOException {
    return ResourceUtils.readResourceArchiveFileAsStream(resourceName);
  }
  
  @Override
  public Iterable<BufferedInputStream> readResourceArchiveAsStream(String resourceName, String relativePath)
      throws IOException {
    return ResourceUtils.readResourceArchiveFileAsStream(resourceName, relativePath);
  }

  @Override
  public Iterator<Record> readResourceTable(String tbl) throws IOException {
    return ResourceUtils.readResourceTable(tbl);
  }

  @Override
  public JobConf getJobConf() {
    return conf;
  }

  @Override
  public Record createMapOutputKeyRecord() throws IOException {
    if (pipeMode && pipeNode != null && pipeNode.getType().equals("map")) {
      return new WritableRecord(pipeNode.getOutputKeySchema());
    } else {
      return new WritableRecord(conf.getMapOutputKeySchema());
    }
  }

  @Override
  public Record createMapOutputValueRecord() throws IOException {
    if (pipeMode && pipeNode != null && pipeNode.getType().equals("map")) {
      return new WritableRecord(pipeNode.getOutputValueSchema());
    } else {
      return new WritableRecord(conf.getMapOutputValueSchema());
    }
  }

  /**
   * for pipeline mode
   */
  @Override
  public Record createOutputKeyRecord() throws IOException {
    if (pipeMode && pipeNode != null) {
      return new WritableRecord(pipeNode.getOutputKeySchema());
    } else {
      return null;
    }
  }

  /**
   * for pipeline mode
   */
  @Override
  public Record createOutputValueRecord() throws IOException {
    if (pipeMode && pipeNode != null) {
      return new WritableRecord(pipeNode.getOutputValueSchema());
    } else {
      return null;
    }
  }

  public boolean isPipelineMode() {
    return this.pipeMode;
  }

  public Pipeline getPipeline() {
    return this.pipeline;
  }

  public TransformNode getCurrentNode() {
    return this.pipeNode;
  }

  @Override
  public TableInfo[] getOutputTableInfo() {
    return OutputUtils.getTables(conf);
  }

  /**
   * ***********************************
   * Support for multi insert and inner output
   *
   * ************************************
   */
  protected Map<String, Integer> label2offset = new HashMap<String, Integer>();
  protected Column[] packagedOutputSchema;
  private static final String MULTIDEST_LABEL = "MULTIDEST_LABEL";
  private static final String INNEROUTPUT_LABEL = "INNEROUTPUT_LABEL";
  private static final String PARTITION_ID = "__partition_id__";
  protected int innerOutputIndex = 0;

  private void initOutputSchema() {
    TableInfo[] tables = getOutputTableInfo();
    if (tables == null) {
      packagedOutputSchema = new Column[]{new Column("nil", OdpsType.STRING)};
      return;
    }

    List<Column[]> schemas = new ArrayList<Column[]>();
    List<OdpsType> outputColumnTypes = new ArrayList<OdpsType>();
    boolean multiInsert = tables.length > 1;
    int length = 0;
    for (TableInfo t : tables) {
      Column[] output;
      if (t.getLabel() == null) {
        output = conf.getOutputSchema();
      } else {
        output = conf.getOutputSchema(t.getLabel());
      }

      List<OdpsType> tbColumnTypes = new ArrayList<OdpsType>();
      for (Column col : output) {
        tbColumnTypes.add(col.getType());
      }
      // check if the same columns already exists
      int idx = Collections.indexOfSubList(outputColumnTypes, tbColumnTypes);
      if (idx >= 0) {
        // merge columns for tableinfos with the same schema
        label2offset.put(t.getLabel(), idx);
        continue;
      }
      label2offset.put(t.getLabel(), length);

      for (Column col : output) {
        outputColumnTypes.add(col.getType());
      }
      length += output.length;
      schemas.add(output);
    }
    // If multi insert, add 1 additional label field
    length += (multiInsert ? 1 : 0);
    // If inner output, add 1 additional label field
    length += (innerOutput ? 1 : 0);
    Column[] outputFields = new Column[length];
    length = 0;
    for (Column[] r : schemas) {
      for (Column f : r) {
        outputFields[length] = f;
        length++;
      }
    }
    if (multiInsert) {
      outputFields[length] = new Column(MULTIDEST_LABEL, OdpsType.STRING);
      length++;
    }
    if (innerOutput) {
      outputFields[length] = new Column(INNEROUTPUT_LABEL, OdpsType.STRING);
    }
    packagedOutputSchema = outputFields;
  }

  protected Column[] getPackagedOutputSchema() {
    return packagedOutputSchema;
  }

  protected Column[] getIntermediateOutputSchema() {
    Column[] intermediateFields = (Column[]) ArrayUtils.addAll(getMapOutputKeySchema(),
                                                               getMapOutputValueSchema());
    if (getPartitioner() != null) {
      intermediateFields = (Column[]) ArrayUtils.addAll(SchemaUtils.fromString(String.format("%s:BIGINT", PARTITION_ID)), intermediateFields);
    }
    return intermediateFields;
  }

  protected Column[] getPipelineOutputSchema(int nodeId) {
    Column[] intermediateFields = (Column[]) ArrayUtils.addAll(getMapOutputKeySchema4Pipeline(nodeId),
                                                               getMapOutputValueSchema4Pipeline(nodeId));
    if (getPartitioner(nodeId) != null) {
      intermediateFields = (Column[]) ArrayUtils.addAll(SchemaUtils.fromString(String.format("%s:BIGINT", PARTITION_ID)), intermediateFields);
    }
    return intermediateFields;
  }

  protected Class getPartitioner() {
    return conf.getPartitionerClass();
  }

  protected String getPartitioner(int nodeId) {
    return conf.getPartitioner(nodeId);
  }

  @Override
  public String[] getGroupingColumns() {
    return conf.getOutputGroupingColumns();
  }

  protected void configure(ExecutionContext ctx) {
    taskId = new TaskId(ctx.getStageID(), ctx.getWorkerID());
    this.ctx = ctx;
    if (this.pipeMode) {
      // TODO: hack task index here
      String tid = getTaskID().toString();
      System.out.println("Task ID: " + tid);
      this.pipeIndex = Integer.parseInt(tid.split("_")[0].substring(1)) - 1;
      String exeMode = conf.get(SessionState.MR_EXECUTION_MODE);
      if (exeMode != null && (exeMode.equalsIgnoreCase("sql") || exeMode.equalsIgnoreCase("hybrid"))) {
        TableInfo[] infos = InputUtils.getTables(conf);
        if (infos != null && infos.length > 1 && this.pipeIndex > 0) {
          this.pipeIndex = this.pipeIndex - infos.length + 1;
          this.pipeIndex = this.pipeIndex < 0 ? 0 :this.pipeIndex;
        }
      }
      resolveSqlModeNodeIndex(exeMode, tid);
      this.pipeNode = pipeline.getNode(pipeIndex);
    }

    if (pipeMode && pipeNode != null) {
      if (pipeNode.getNextNode() != null) {
        reducerNum = pipeNode.getNextNode().getNumTasks();
      } else if (pipeIndex > 0) {
        // the last but not the first node of pipeline, must be reduce node
        reducerNum = pipeNode.getNumTasks();
      } else {
        reducerNum = 0;
      }
    } else {
      reducerNum = conf.getNumReduceTasks();
    }
  }

  private void resolveSqlModeNodeIndex(String exeMode, String tid) {
    if (exeMode != null && exeMode.equalsIgnoreCase("lot") &&
        (conf.get("odps.mr.project.disable") == null || !Boolean.parseBoolean(conf.get("odps.mr.project.disable")))) {
      return;
    }
    TableInfo[] infos = InputUtils.getTables(conf);
    if (infos == null || infos.length < 2) {
      return;
    }
    String projectConfMode = "";
    if (conf.get("odps.mr.project.conf") != null) {
      projectConfMode = conf.get("odps.mr.project.conf").toLowerCase();
    }
    if (projectConfMode.equals("lot")) {
      return;
    }
    if (tid.startsWith("M")) {
      this.pipeIndex = 0;
    } else {
      int pipePrefix = Integer.parseInt(tid.split("_")[0].substring(1));
      String taskIdPrefix = "R" + pipePrefix;
      int prefixLength = pipePrefix > 9 ? 9 : pipePrefix;
      for (int i = 1; i < prefixLength; i++) {
        taskIdPrefix += "_" + i;
      }
      if (tid.startsWith(taskIdPrefix)) {
        this.pipeIndex = 1;
      } else if (tid.startsWith("R3_2_")) {
        this.pipeIndex = 2;
      } else if (this.pipeIndex > 2 &&
              (projectConfMode.equals("sql") || projectConfMode.equals("hybrid"))) {
        this.pipeIndex = pipePrefix - infos.length;
        this.pipeIndex = this.pipeIndex < 0 ? 0 : this.pipeIndex;
      }
    }
  }

  protected boolean hasLabel(String label) {
    if (label2offset.get(label) == null) {
      return false;
    }
    return true;
  }

  protected Writable[] createOutputRow(Record r, String label) {
    if (label2offset.size() == 1 && !innerOutput) {
      // not a multi-insert
      return ((WritableRecord) r).toWritableArray();
    } else {
      // assert validateLabel(label) == true;
      Writable[] row = new Writable[packagedOutputSchema.length];
      Writable[] output = ((WritableRecord) r).toWritableArray();
      System.arraycopy(output, 0, row, label2offset.get(label), output.length);
      int index = row.length - 1;
      if (innerOutput) {
        row[index] = new Text("INNER_OUTPUT");
        index--;
      }
      if (label2offset.size() > 1) {
        row[index] = new Text(label);
      }
      return row;
    }
  }

  protected Writable[] createInnerOutputRow(Writable[] w, boolean isInnerOutput, String innerOutputLabel, String multiOutputLabel) {
    if (label2offset.size() == 1 && !innerOutput) {
      // not a multi-insert
      return w;
    } else {
      // assert validateLabel(label) == true;
      Writable[] row = new Writable[packagedOutputSchema.length];
      Writable[] output = w;
      if (isInnerOutput) {
        System.arraycopy(output, 0, row, innerOutputIndex + label2offset.get(multiOutputLabel), output.length);
      } else {
        System.arraycopy(output, 0, row, 0, output.length);
      }
      int index = row.length - 1;
      if (innerOutput) {
        row[index] = new Text(innerOutputLabel);
        index--;
      }
      if (label2offset.size() > 1) {
        row[index] = new Text(multiOutputLabel);
      }
      return row;
    }
  }

  protected long getNextCntr(long cntr, boolean isMem) {
    long statBase = isMem ? 10000 : 1000000;
    if (cntr >= statBase) {
      return cntr + statBase;
    }

    return 10 * cntr;
  }

  @Override
  public VolumeInfo getInputVolumeInfo() throws IOException {
    return ctx.getInputVolumeInfo();
  }

  @Override
  public VolumeInfo getInputVolumeInfo(String label) throws IOException {
    return ctx.getInputVolumeInfo(label);
  }

  @Override
  public VolumeInfo getOutputVolumeInfo() throws IOException {
    return ctx.getOutputVolumeInfo();
  }

  @Override
  public VolumeInfo getOutputVolumeInfo(String label) throws IOException {
    return ctx.getOutputVolumeInfo(label);
  }

  @Override
  public com.aliyun.odps.volume.FileSystem getInputVolumeFileSystem() throws IOException {
    return ctx.getInputVolumeFileSystem();
  }

  @Override
  public com.aliyun.odps.volume.FileSystem getInputVolumeFileSystem(String label)
      throws IOException {
    return ctx.getInputVolumeFileSystem(label);
  }

  @Override
  public com.aliyun.odps.volume.FileSystem getOutputVolumeFileSystem() throws IOException {
    return ctx.getOutputVolumeFileSystem();
  }

  @Override
  public com.aliyun.odps.volume.FileSystem getOutputVolumeFileSystem(String label)
      throws IOException {
    return ctx.getOutputVolumeFileSystem(label);
  }

  @Override
  public com.aliyun.odps.volume.FileSystem getTempFileSystem() throws IOException {
    if (conf.getBoolean("odps.mapred.enable.user.tempfile", false)) {
      return ctx.getTempFileSystem();
    }
    throw new UnsupportedOperationException("user tempfile disabled");
  }
}
