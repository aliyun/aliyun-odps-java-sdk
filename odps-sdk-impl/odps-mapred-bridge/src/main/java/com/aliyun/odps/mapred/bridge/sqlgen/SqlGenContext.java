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

package com.aliyun.odps.mapred.bridge.sqlgen;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.bridge.utils.TypeUtils;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.pipeline.Pipeline;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;

public class SqlGenContext {

  private JobConf job;
  private String id;

  private static final String MULTIDEST_LABEL = "MULTIDEST_LABEL";
  private static final String INNEROUTPUT_LABEL = "INNEROUTPUT_LABEL";
  private static final String KEY_PREFIX = "k_";
  private static final String VALUE_PREFIX = "v_";
  private static final String PARTITION_ID = "__partition_id__";
  private Column[] packagedOutputSchema;
  private List<TableInfo> outputTableInfos = new ArrayList<TableInfo>();
  private Pipeline pipeline;
  private boolean pipeMode;

  public static String getMultidestLabel() {
    return MULTIDEST_LABEL;
  }

  public TableInfo[] getInputTableInfos() {
    TableInfo[] tableInfos = InputUtils.getTables(job);
    if (tableInfos == null || tableInfos.length == 0) {
      return tableInfos;
    }

    for (TableInfo tableInfo : tableInfos) {
      if (tableInfo.getTableName() != null && tableInfo.getProjectName() != null) {
        Column[] columns = ((BridgeJobConf) job).getInputSchema(tableInfo);
        if (columns != null && columns.length > 0) {
          tableInfo.setCols(SchemaUtils.getNames(columns));
        }
      }
    }
    return tableInfos;
  }

  public TableInfo[] getOutputTableInfos() {
    return OutputUtils.getTables(job);
  }

  public boolean isNilOutput() {
    return OutputUtils.getTables(job) == null;
  }

  public String getIntermediateColsJoined() {
    Column[] intermediateKeySchema = job.getMapOutputKeySchema();
    Column[] intermediateValueSchema = job.getMapOutputValueSchema();
    StringBuilder rt = new StringBuilder();
    if (intermediateKeySchema != null && intermediateKeySchema.length > 0) {
      for (String name : SchemaUtils.getNames(intermediateKeySchema)) {
        if (rt.length() > 0) {
          rt.append(',');
        }
        rt.append(KEY_PREFIX + name);
      }
    }
    if (intermediateValueSchema != null && intermediateValueSchema.length > 0) {
      for (String name : SchemaUtils.getNames(intermediateValueSchema)) {
        if (rt.length() > 0) {
          rt.append(',');
        }
        rt.append(VALUE_PREFIX + name);
      }
    }
    return rt.toString();
  }

  public String getIntermediateColsJoinedMapOut() {
    StringBuilder rt = new StringBuilder();
    if (job.getPartitionerClass() != null) {
      rt.append(PARTITION_ID);
      rt.append(',');
    }
    rt.append(getIntermediateColsJoined());
    return rt.toString();
  }

  private String getPrefixedJoinedString(String[] colList, String prefix) {
    StringBuilder sb = new StringBuilder();
    for (String s : colList) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(prefix).append(s);
    }
    return sb.toString();
  }

  public boolean mapOnly() {
    return this.pipeMode ? pipeline.getNodeNum() == 1 : job.getNumReduceTasks() == 0;
  }

  public String getPartitionColsJoined() {
    if (job.getPartitionerClass() != null) {
      return PARTITION_ID;
    }
    return getPrefixedJoinedString(job.getPartitionColumns(), KEY_PREFIX);
  }

  public String getSortColsJoined() {
    String[] cols = job.getOutputKeySortColumns();
    JobConf.SortOrder[] order = job.getOutputKeySortOrder();
    assert cols.length == order.length;

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < cols.length; i++) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(KEY_PREFIX).append(cols[i]).append(" ").append(order[i].toString());
    }
    return sb.toString();
  }

  public boolean multiInsert() {
    return getOutputTableInfos().length > 1;
  }

  public String getId() {
    return id;
  }

  public String getMapStreamProcessor() {
    String cmd = job.getStreamProcessor("map");
    if (cmd != null) {
      try {
        return URLDecoder.decode(cmd, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  public String getReduceStreamProcessor() {
    String cmd = job.getStreamProcessor("reduce");
    if (cmd != null) {
      try {
        return URLDecoder.decode(cmd, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  public String getFunctionCreateText() {
    return job.getFunctionCreateText();
  }

  public String getFunctionDropText() {
    return job.getFunctionDropText();
  }

  public List<TableInfo> getMultiOutputTableInfos() {
    return this.outputTableInfos;
  }

  public String getPackagedColsJoined() {
    if (this.outputTableInfos.isEmpty()) {
      initOutputSchema();
    }
    StringBuilder rt = new StringBuilder();
    if (packagedOutputSchema != null && packagedOutputSchema.length >0) {
      for (String name : SchemaUtils.getNames(packagedOutputSchema)) {
        if (rt.length() > 0) {
          rt.append(',');
        }
        rt.append(name);
      }
    }
    return rt.toString();
  }

  void initOutputSchema() {
    TableInfo[] tables = OutputUtils.getTables(job);
    if (tables == null || tables.length == 0) {
      packagedOutputSchema = new Column[]{new Column("nil", OdpsType.STRING)};
      return;
    }
    if (tables.length == 1) {
      outputTableInfos.add(tables[0]);
      if (tables[0].getLabel() == null) {
        packagedOutputSchema = job.getOutputSchema();
      } else {
        packagedOutputSchema = job.getOutputSchema(tables[0].getLabel());
      }
      return;
    }

    boolean innerOutput = job.getInnerOutputEnable();
    List<Column> outputColumns = new ArrayList<Column>();
    List<OdpsType> outputColumnTypes = new ArrayList<OdpsType>();
    Map<String, Integer> label2offset = new HashMap<String, Integer>();
    int length = 0;
    int tableIndex = 1;
    for (TableInfo t : tables) {
      Column[] output;
      if (t.getLabel() == null) {
        output = job.getOutputSchema();
      } else {
        output = job.getOutputSchema(t.getLabel());
      }
      List<Column> tableColumns = new ArrayList<Column>();
      List<OdpsType> tbColumnTypes = new ArrayList<OdpsType>();
      for (Column col : output) {
        tbColumnTypes.add(col.getType());
      }
      int idx = Collections.indexOfSubList(outputColumnTypes, tbColumnTypes);
      if (idx >= 0) {
        label2offset.put(t.getLabel(), idx);
        Column [] tableCols = new Column[output.length];
        tableColumns = new ArrayList<Column>(outputColumns.subList(idx, idx + output.length));
        tableColumns.toArray(tableCols);
        t.setCols(SchemaUtils.getNames(tableCols));
        outputTableInfos.add(t);
        continue;
      }
      label2offset.put(t.getLabel(), length);
      for (Column col : output) {
        outputColumnTypes.add(col.getType());
        String colName = String.format("output%d_%s", tableIndex, col.getName());
        tableColumns.add(TypeUtils.createColumnWithNewName(colName, col));
        outputColumns.add(TypeUtils.createColumnWithNewName(colName, col));
      }
      length += output.length;
      tableIndex++;
      Column [] tableCols = new Column[tableColumns.size()];
      tableColumns.toArray(tableCols);
      t.setCols(SchemaUtils.getNames(tableCols));
      outputTableInfos.add(t);
    }
    length += (innerOutput ? 2 : 1);
    Column[] outputFields = new Column[length];
    length = 0;
    for (Column f : outputColumns) {
      outputFields[length] = f;
      length++;
    }
    outputFields[length] = new Column(MULTIDEST_LABEL, OdpsType.STRING);
    length++;
    if (innerOutput) {
      outputFields[length] = new Column(INNEROUTPUT_LABEL, OdpsType.STRING);
    }
    packagedOutputSchema = outputFields;
  }

  public boolean isNoInputTableInfos() {
    return InputUtils.getTables(job) == null;
  }

  public boolean isOutputOverwrite() {
    boolean isInnerOutput = job.getInnerOutputEnable();
    return isInnerOutput ? false : job.getOutputOverwrite();
  }

  public String getMapInputSeparator() {
    return getSeparator("map", "input");
  }

  public String getMapOutputSeparator() {
    return getSeparator("map", "output");
  }

  public String getReduceInputSeparator() {
    return getSeparator("reduce", "input");
  }

  public String getReduceOutputSeparator() {
    return getSeparator("reduce", "output");
  }

  String getSeparator(String taskType, String dataType) {
    String field = job.get(String.format("stream.%s.%s.field.separator", taskType, dataType));
    String record = job.get(String.format("stream.%s.%s.record.separator", taskType, dataType));
    if (field != null && record != null) {
      return String.format("ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s' LINES SEPARATED BY '%s' ", field, record);
    } else if (field != null) {
      return String.format("ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s' ", field);
    } else if (record != null) {
      return String.format("ROW FORMAT DELIMITED LINES SEPARATED BY '%s' ", record);
    }
    return null;
  }

  public SqlGenContext(JobConf conf, String id) {
    this.job = conf;
    this.id = id;
  }

  public SqlGenContext(JobConf conf, String id, Pipeline pipeline) {
    this.job = conf;
    this.id = id;
    this.pipeline = pipeline;
    if (this.pipeline != null) {
      this.pipeMode = true;
    }
  }

}
