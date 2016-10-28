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

package com.aliyun.odps.mapred.local.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.mapred.bridge.ErrorCode;
import com.aliyun.odps.mapred.bridge.utils.Validator;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;

public class LocalValidatorFactory {
  private final static int MAX_INPUT_TABLE_COUNT = 1024;
  private final static int MAX_DISTINCT_INPUT_TABLE = 64;
  private final static int MAX_RESOURCE_COUNT = 256;
  private final static int MAX_MAP_INSTANCE_COUNT = 99999;

  private final static int MIN_MEMORY_FOR_MAP_TAK = 256; // 256M
  private final static int MAX_MEMORY_FOR_MAP_TAK = 12 * 1024; // 12G
  private final static int MIN_MEMORY_FOR_REDUCE_TAK = 256; // 256M
  private final static int MAX_MEMORY_FOR_REDUCE_TAK = 12 * 1024; // 12G
  private final static int MIN_MEMORY_FOR_JVM = 256; // 256M
  private final static int MAX_MEMORY_FOR_JVM = 12 * 1024; // 12G

  private final static int MIN_FUNCTION_TIME_OUT = 1;
  private final static int MAX_FUNCTION_TIME_OUT = 3600;

  private final static int MIN_PRIORITY_VALUE = 0;
  private final static int MAX_PRIORITY_VALUE = 9;


  private static boolean validateColumns(String[] columns, Column[] schema, StringBuilder errorMsg) {
    Set<String> schemaColums = new HashSet<String>();
    for (int i = 0; i < schema.length; ++i) {
      schemaColums.add(schema[i].getName());
    }
    for (int i = 0; i < columns.length; ++i) {
      if (!schemaColums.contains(columns[i])) {
        errorMsg.append("Can't find column " + columns[i] + " from key schema.");
        return false;
      }
    }
    return true;
  }

  static void throwException(ErrorCode e) throws OdpsException {
    throw new OdpsException(e.code, e.msg);
  }

  static void throwException(ErrorCode e, String info) throws OdpsException {
    throw new OdpsException(e.code, e.msg + ":" + info);
  }

  public static void throwException(ErrorCode e, Throwable cause) throws OdpsException {
    throw new OdpsException(e.code, e.msg, cause);

  }

  static class CompositeValidator implements Validator {

    List<Validator> validators = new ArrayList<Validator>();

    public void addValidator(Validator v) {
      validators.add(v);
    }

    @Override
    public void validate() throws OdpsException {
      for (Validator v : validators) {
        v.validate();
      }
    }
  }

  /**
   * Meta Validator.
   *
   * Validate the input/output tables available.
   */
  static class InputOutputValidator implements Validator {

    private BridgeJobConf job;

    public InputOutputValidator(BridgeJobConf job) {
      this.job = job;

    }

    private void validateTable(TableInfo table, Map<String, TableMeta> distinctTables)
        throws OdpsException {

      TableMeta tableDesc = distinctTables.get(table.getProjectName() + "." + table.getTableName());
      if (tableDesc == null) {
        if (!WareHouse.getInstance().existsTable(table.getProjectName(), table.getTableName())) {
          throwException(ErrorCode.TABLE_NOT_FOUND, table.toString());
        }

        tableDesc = WareHouse.getInstance().getTableMeta(table.getProjectName(),
                                                         table.getTableName());

        distinctTables.put(table.getProjectName() + "." + table.getTableName(), tableDesc);
      }

      // Check if part spec is valid
      Map<String, String> partSpec = table.getPartSpec();
      if (partSpec != null && partSpec.size() > 0) {
        Column[] cols = tableDesc.getPartitions();
        List<String> colNames = new ArrayList<String>();
        for (Column c : cols) {
          colNames.add(c.getName());
        }
        for (String key : partSpec.keySet()) {
          if (!colNames.contains(key.toLowerCase())) {
            throwException(ErrorCode.PARTITION_COLUMN_NOT_FOUND, key);
          }
        }
      }

      // Check if select columns exist in source table.
      if (table.getCols() != null) {
        Column[] schema = tableDesc.getCols();
        HashSet<String> names = new HashSet<String>(Arrays.asList(table.getCols()));
        names.removeAll(Arrays.asList(SchemaUtils.getNames(schema)));
        if (!names.isEmpty()) {
          throwException(ErrorCode.COLUMN_NOT_FOUND, StringUtils.join(names, ","));
        }
      }
    }

    @Override
    public void validate() throws OdpsException {
      // Validate inputs
      TableInfo[] tables = InputUtils.getTables(job);
      if (tables == null || tables.length < 1) {
        return;
      }

      if (tables.length > MAX_INPUT_TABLE_COUNT) {
        throwException(ErrorCode.TOO_MANY_INPUT_TABLE, "Expecting no more than 1024 partitions. ");
      }
      Map<String, TableMeta> distinctInputTables = new HashMap<String, TableMeta>();
      for (TableInfo table : tables) {
        validateTable(table, distinctInputTables);
        if (distinctInputTables.size() > MAX_DISTINCT_INPUT_TABLE) {
          throwException(ErrorCode.TOO_MANY_INPUT_TABLE,
                         "Expecting no more than 64 distinct tables. ");
        }
      }

      // Validate outputs
      tables = OutputUtils.getTables(job);
      if (tables == null || tables.length < 1) {
        // No output table is allowed
        return;
      }

      Map<String, TableMeta> distinctOutputTables = new HashMap<String, TableMeta>();
      Set<String> labelNames = new HashSet<String>();
      for (TableInfo table : tables) {
        validateTable(table, distinctOutputTables);
        if (labelNames.contains(table.getLabel())) {
          throwException(ErrorCode.OUTPUT_LABEL_NOT_UNIQUE, table.getLabel());
        }
        labelNames.add(table.getLabel());
      }
    }
  }

  /**
   * Config Validator.
   *
   * Validate the job configuration.
   */
  static class ConfigValidator implements Validator {

    private JobConf job;

    public ConfigValidator(JobConf job) {
      this.job = job;
    }

    private boolean between(long i, long l, long h) {
      if (i > h || i < l) {
        return false;
      }
      return true;
    }

    @Override
    public void validate() throws OdpsException {
      if (job.get("odps.mapred.map.class") == null) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Mapper class not specified.");
      }

      if (InputUtils.getTables(job) == null
          && !between(job.getNumMapTasks(), 0, MAX_MAP_INSTANCE_COUNT)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Map tasks " + job.getNumMapTasks()
            + " out of bound, shoud be in [0, " + MAX_MAP_INSTANCE_COUNT + "] range.");
      }

      if (job.getNumReduceTasks() != 0 && job.get("odps.mapred.reduce.class") == null) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Reducer class not specified.");
      }

      if (job.getNumReduceTasks() != 0 && !validateSchema(job.getMapOutputKeySchema())) {
        throwException(ErrorCode.ILLEGAL_CONFIG,
                       "Malformed map output key schema:" + job
                           .get("odps.mapred.mapoutput.key.schema"));
      }
      if (job.getNumReduceTasks() != 0 && !validateSchema(job.getMapOutputValueSchema())) {
        throwException(ErrorCode.ILLEGAL_CONFIG,
                       "Malformed map output value schema:" + job
                           .get("odps.mapred.mapoutput.value.schema"));
      }

      if (!between(job.getMemoryForMapTask(), MIN_MEMORY_FOR_MAP_TAK, MAX_MEMORY_FOR_MAP_TAK)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Memory for map " + job.getMemoryForMapTask()
            + " out of bound, should be in [" + MIN_MEMORY_FOR_MAP_TAK + ", "
            + MAX_MEMORY_FOR_MAP_TAK + "] range.");
      }
      if (!between(job.getMemoryForReduceTask(), MIN_MEMORY_FOR_REDUCE_TAK,
          MAX_MEMORY_FOR_REDUCE_TAK)) {
        throwException(ErrorCode.ILLEGAL_CONFIG,
            "Memory for reduce " + job.getMemoryForReduceTask() + " out of bound, should be in ["
                + MIN_MEMORY_FOR_REDUCE_TAK + ", " + MAX_MEMORY_FOR_REDUCE_TAK + "] range.");
      }
      if (!between(job.getMemoryForJVM(), MIN_MEMORY_FOR_JVM, MAX_MEMORY_FOR_JVM)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Memory for jvm " + job.getMemoryForJVM()
            + " out of bound, should be in [" + MIN_MEMORY_FOR_JVM + ", " + MAX_MEMORY_FOR_JVM
            + "] range.");
      }
      if (job.getOutputKeySortColumns().length != job.getOutputKeySortOrder().length) {
        throwException(
            ErrorCode.ILLEGAL_CONFIG,
            "Key sort columns length should match key sort order length. Sort columns are "
            + Arrays.toString(job.getOutputKeySortColumns()) + " but sort order is "
            + Arrays.toString(job.getOutputKeySortOrder()));
      }
      StringBuilder errorMsg = new StringBuilder();
      if (!validatePartitionColumns(job, errorMsg)) {
        throwException(ErrorCode.ILLEGAL_CONFIG,
            "Key partition columns should be inside of output key columns. " + errorMsg);
      }
      if (!between(job.getFunctionTimeout(), MIN_FUNCTION_TIME_OUT, MAX_FUNCTION_TIME_OUT)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Function timeout " + job.getFunctionTimeout()
            + " out of bound, should be in [" + MIN_FUNCTION_TIME_OUT + ", "
            + MAX_FUNCTION_TIME_OUT + "] range.");
      }
      if (!between(job.getInstancePriority(), MIN_PRIORITY_VALUE, MAX_PRIORITY_VALUE)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Instance priority" + job.getInstancePriority()
            + " out of bound, should be in [" + MIN_PRIORITY_VALUE + ", " + MAX_PRIORITY_VALUE
            + "] range.");
      }
    }

    private boolean validatePartitionColumns(JobConf job, StringBuilder errorMsg) {
      if (job.getNumReduceTasks() > 0 && job.getPartitionerClass() == null) {
          return validateColumns(job.getPartitionColumns(), job.getMapOutputKeySchema(), errorMsg);
      }

      return true;
    }

    private boolean validateSchema(Column[] schema) {
      if (schema == null || schema.length < 1) {
        return false;
      }
      return true;
    }
  }

  /**
   * Resource Validator.
   *
   * Validate the resources.
   */
  static class ResourceValidator implements Validator {

    private JobConf job;

    public ResourceValidator(JobConf job) {
      this.job = job;
    }

    @Override
    public void validate() throws OdpsException {
      // Check if resource exist
      String[] res = job.getResources();
      if (res == null || res.length <= 0) {
        return;
      }
      if (res.length > MAX_RESOURCE_COUNT) {
        throwException(ErrorCode.TOO_MANY_RESOURCE_ITEMS,
                       "Acturally " + String.valueOf(res.length));
      }
    }
  }

  static class PipelineValidtor implements Validator {

    private Pipeline pipeline;

    public PipelineValidtor(JobConf job) {
      this.pipeline = Pipeline.fromJobConf(job);
    }

    @Override
    public void validate() throws OdpsException {
      if (pipeline == null) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Pipeline not specified.");
      }
      if (pipeline.getNodeNum() == 0) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Pipeline is empty.");
      }
      if (pipeline.getFirstNode().getType().equals("reduce")) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "First operator of pipeline must be mapper");
      }
      for (int i = 0; i < pipeline.getNodeNum(); i++) {
        if (i > 0 && pipeline.getNode(i).getType().equals("map")) {
          throwException(ErrorCode.ILLEGAL_CONFIG, "Operators after first node must be reducer");
        }
        if (i < pipeline.getNodeNum() - 1) {
          if (pipeline.getNode(i).getOutputKeySchema() == null) {
            throwException(ErrorCode.ILLEGAL_CONFIG, "Operator ouput key schema not set");
          }
          if (pipeline.getNode(i).getOutputValueSchema() == null) {
            throwException(ErrorCode.ILLEGAL_CONFIG, "Operator ouput value schema not set");
          }
        }
      }
      StringBuilder errorMsg = new StringBuilder();
      if (!validatePartitionColumns(pipeline, errorMsg)) {
        throwException(ErrorCode.ILLEGAL_CONFIG,
            "Key partition columns should be inside of output key columns. " + errorMsg);
      }
    }

    private boolean validatePartitionColumns(Pipeline pipeline, StringBuilder errorMsg) {
      TransformNode node = null;
      for (int i = 0; i < pipeline.getNodeNum() - 1; ++i) {
        node = pipeline.getNode(i);
        if (node.getPartitionerClass() == null
            && !validateColumns(node.getPartitionColumns(), node.getOutputKeySchema(), errorMsg)) {
          return false;
        }
      }

      return true;
    }

  }

  public static Validator getValidator(BridgeJobConf job) {
    CompositeValidator validator = new CompositeValidator();
    if (Pipeline.fromJobConf(job) == null) {
      validator.addValidator(new ConfigValidator(job));
    } else {
      validator.addValidator(new PipelineValidtor(job));
    }
    validator.addValidator(new InputOutputValidator(job));
    validator.addValidator(new ResourceValidator(job));
    return validator;

  }

}
