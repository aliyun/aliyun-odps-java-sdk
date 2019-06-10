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

package com.aliyun.odps.mapred.bridge.utils;

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
import com.aliyun.odps.Table;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.bridge.ErrorCode;
import com.aliyun.odps.mapred.bridge.MetaExplorer;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;

public class ValidatorFactory {

  private static boolean between(long i, long l, long h) {
    if (i > h || i < l) {
      return false;
    }
    return true;
  }

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

    private JobConf job;
    private MetaExplorer explorer;

    public InputOutputValidator(JobConf job, MetaExplorer explorer) {
      this.job = job;
      this.explorer = explorer;
    }

    /**
     * Validate if table
     *
     * 1. Exists
     * 2. Not a view
     * 3. Partition column exists (if applicable)
     * 4. Column exists (if applicable)
     *
     * @param table
     *     The table
     * @param distinctTables
     *     Cache tables in the same group.
     * @throws OdpsException
     */
    private void validateTable(TableInfo table, Map<String, Table> distinctTables, boolean isInput)
        throws OdpsException {

      Table tableDesc = distinctTables.get(
          table.getProjectName() + "." + table.getTableName());
      if (tableDesc == null) {
        // check if table exists
        if (!explorer.existsTable(table.getProjectName(), table.getTableName())) {
          throwException(ErrorCode.TABLE_NOT_FOUND, table.toString());
        }

        // view is now unsupported
        tableDesc = explorer.getTable(table.getProjectName(),
                                      table.getTableName());
        if (tableDesc.isVirtualView() && (!isInput || job.isPipeline() || (InputUtils.getTables(job).length > 1))) {
          throwException(ErrorCode.VIEW_TABLE, table.toString());
        }
        distinctTables.put(table.getProjectName() + "." + table.getTableName(),
                           tableDesc);
      }

      // Check if part spec is valid
      Map<String, String> partSpec = table.getPartSpec();
      if (partSpec != null) {
        List<Column> cols = tableDesc.getSchema().getPartitionColumns();
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
        List<Column> schema = tableDesc.getSchema().getColumns();
        HashSet<String> names = new HashSet<String>(Arrays.asList(table.getCols()));
        names.removeAll(Arrays.asList(SchemaUtils.getNames(schema.toArray(new Column[schema
            .size()]))));
        if (!names.isEmpty()) {
          throwException(ErrorCode.COLUMN_NOT_FOUND, StringUtils.join(names, ","));
        }
      }
    }

    /**
     * Validate if volume exists and is not duplicated with other volume in the same group
     *
     * @param volume
     *     Volume
     * @param distinctVolumes
     *     Cache volumes in the same group
     * @throws OdpsException
     */
    private void validateVolume(VolumeInfo volume,
                                Set<String> distinctVolumes) throws OdpsException {
      String volumeDesc =
          volume.getProjectName() + "." + volume.getVolumeName() + "." + volume
              .getPartSpec();
      if (distinctVolumes.contains(volumeDesc)) {
        throwException(ErrorCode.DUPLICATED_VOLUME_FOUND, volume.toString());
      }
      distinctVolumes.add(volumeDesc);
      // Check if volume spec is valid
      if (volume.getProjectName() == null) {
        volume.setProjectName(explorer.getDefaultProject());
      }
      if (volume.getVolumeName() == null || volume.getVolumeName().isEmpty()) {
        throwException(ErrorCode.MALFORMED_VOLUME_SPEC,
                       "volume name not specified");
      }
      if (volume.getPartSpec() == null || volume.getPartSpec().isEmpty()) {
        throwException(ErrorCode.MALFORMED_VOLUME_SPEC,
                       "volume partition not specified");
      }

      // Check if volume exists
      if (!explorer.existsVolume(volume.getProjectName(),
                                 volume.getVolumeName())) {
        throwException(ErrorCode.VOLUME_NOT_FOUND, volume.toString());
      }
      // Check if label is valid
      if (!between(volume.getLabel().length(), 3, 32) || !volume.getLabel()
          .matches("[A-Z,a-z,0-9,_,#,\\.,\\-]*")) {
        throwException(ErrorCode.MALFORMED_VOLUME_SPEC,
                       "illegal label " + volume.getLabel()
                       + ", expecting [A-Z,a-z,0-9,_,#,\\.,\\-]* with length of [3,32]");
      }
    }

    private void validateTables(TableInfo[] tables)
        throws OdpsException {
      Map<String, Table> distinctOutputTables = new HashMap<String, Table>();
      Set<String> labelNames = new HashSet<String>();
      for (TableInfo table : tables) {
        validateTable(table, distinctOutputTables, false);
        if (labelNames.contains(table.getLabel())) {
          throwException(ErrorCode.OUTPUT_LABEL_NOT_UNIQUE, table.getLabel());
        }
        labelNames.add(table.getLabel());
      }
    }

    private void validateVolumes(VolumeInfo[] volumes) throws OdpsException {
      Set<String> distinctVolumes = new HashSet<String>();
      Set<String> labelNames = new HashSet<String>();
      for (VolumeInfo volume : volumes) {
        validateVolume(volume, distinctVolumes);
        if (labelNames.contains(volume.getLabel())) {
          throwException(ErrorCode.VOLUME_LABEL_NOT_UNIQUE, volume.getLabel());
        }
        labelNames.add(volume.getLabel());
      }
    }

    @Override
    public void validate() throws OdpsException {
      // Validate inputs
      TableInfo[] tables = InputUtils.getTables(job);
      if (tables != null && tables.length > 0) {
        if (tables.length > 1024) {
          throwException(ErrorCode.TOO_MANY_INPUT_TABLE,
                         "Expecting no more than 1024 partitions. ");
        }
        Map<String, Table> distinctInputTables = new HashMap<String, Table>();
        for (TableInfo table : tables) {
          validateTable(table, distinctInputTables, true);
          if (distinctInputTables.size() > 64) {
            throwException(ErrorCode.TOO_MANY_INPUT_TABLE,
                           "Expecting no more than 64 distinct tables. ");
          }
        }
      }

      // Validate outputs
      tables = OutputUtils.getTables(job);
      if (tables != null && tables.length > 0) {
        validateTables(tables);
      }

      // Validate input volumes
      VolumeInfo[] volumes = InputUtils.getVolumes(job);
      if (volumes != null && volumes.length > 0) {
        if (volumes.length > 1024) {
          throwException(ErrorCode.TOO_MANY_INPUT_VOLUME,
                         "Expecting no more than 1024 partitions. ");
        }
        validateVolumes(volumes);
      }

      // Validate output volumes
      volumes = OutputUtils.getVolumes(job);
      if (volumes != null && volumes.length > 0) {
        if (volumes.length > 1024) {
          throwException(ErrorCode.TOO_MANY_OUTPUT_VOLUME,
                         "Expecting no more than 1024 partitions. ");
        }
        validateVolumes(volumes);
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

    @Override
    public void validate() throws OdpsException {
      if (job.get("odps.mapred.map.class") == null) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Mapper class not specified.");
      }

      if (InputUtils.getTables(job) == null && !between(job.getNumMapTasks(), 0, 99999)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Map tasks " + job.getNumMapTasks()
                                                 + " out of bound, should be in [0, 99999] range.");
      }

      if (!between(job.getNumReduceTasks(), 0, 99999)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Reduce tasks " + job.getNumReduceTasks()
                                                 + " out of bound, should be in [0, 99999] range.");
      }

      if (job.getNumReduceTasks() != 0 && job.get("odps.mapred.reduce.class") == null) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Reducer class not specified.");
      }

      if (job.getNumReduceTasks() != 0 && !validateSchema(job.getMapOutputKeySchema())) {
        throwException(ErrorCode.ILLEGAL_CONFIG,
                       "Malformed map output key schema:" + job.get(
                           "odps.mapred.mapoutput.key.schema"));
      }
      if (job.getNumReduceTasks() != 0 && !validateSchema(job.getMapOutputValueSchema())) {
        throwException(ErrorCode.ILLEGAL_CONFIG,
                       "Malformed map output value schema:" + job.get(
                           "odps.mapred.mapoutput.value.schema"));
      }

      if (!between(job.getMemoryForJVM(), 256, 12 * 1024)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Memory for jvm " + job.getMemoryForJVM()
                                                 + " out of bound, should be in [256, 12288] range.");
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
      if (!between(job.getFunctionTimeout(), 1, 3600)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Function timeout " + job.getFunctionTimeout()
                                                 + " out of bound, should be in [1, 3600] range.");
      }
      if (!between(job.getInstancePriority(), 0, 9)) {
        throwException(ErrorCode.ILLEGAL_CONFIG, "Instance priority" + job.getInstancePriority()
                                                 + " out of bound, should be in [0, 9] range.");
      }
//      if (!between((long)(job.getCombinerCacheSpillPercent()*100), 0, 100)) {
//        throwException(ErrorCode.ILLEGAL_CONFIG, "Combiner Cache Spill Percent" + job.getCombinerCacheSpillPercent()
//                                                 + " out of bound, should be in [0, 1] range.");
//      }
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
    private MetaExplorer explorer;

    public ResourceValidator(JobConf job, MetaExplorer explorer) {
      this.job = job;
      this.explorer = explorer;
    }

    @Override
    public void validate() throws OdpsException {
      // Check if resource exist
      String[] res = job.getResources();
      if (res == null || res.length <= 0) {
        return;
      }
      if (res.length > 256) {
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

  public static Validator getValidator(JobConf job, MetaExplorer explorer) {
    CompositeValidator validator = new CompositeValidator();
    if (Pipeline.fromJobConf(job) == null) {
      validator.addValidator(new ConfigValidator(job));
    } else {
      validator.addValidator(new PipelineValidtor(job));
    }
    validator.addValidator(new InputOutputValidator(job, explorer));
    validator.addValidator(new ResourceValidator(job, explorer));
    return validator;

  }

}
