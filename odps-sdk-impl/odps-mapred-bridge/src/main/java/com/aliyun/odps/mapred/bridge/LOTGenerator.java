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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import com.aliyun.odps.mapred.bridge.utils.TypeUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;

import apsara.odps.ExpressionProtos.Constant;
import apsara.odps.ExpressionProtos.Reference;
import apsara.odps.ExpressionProtos.Null;
import apsara.odps.LanguageProtos.Language;
import apsara.odps.OrderProtos.Order;
import apsara.odps.PartitionSpecProtos.PartitionSpec;
import apsara.odps.TypesProtos;
import apsara.odps.lot.DataSinkProtos.DataSink;
import apsara.odps.lot.DataSourceProtos.DataSource;
import apsara.odps.lot.DistributeByProtos.DistributeBy;
import apsara.odps.lot.ExpressionProtos.ScalarExpression;
import apsara.odps.lot.ExpressionProtos.ScalarFunction;
import apsara.odps.lot.FakeSinkProtos;
import apsara.odps.lot.FilterProtos.Filter;
import apsara.odps.lot.LanguageSourceProtos.LanguageSource;
import apsara.odps.lot.LanguageTransformProtos.LanguageTransform;
import apsara.odps.lot.Lot.LogicalOperator;
import apsara.odps.lot.Lot.LogicalOperatorTree;
import apsara.odps.lot.Lottask.LotTask;
import apsara.odps.lot.SchemaProtos.Schema;
import apsara.odps.lot.SelectProtos.Select;
import apsara.odps.lot.SortByProtos.SortBy;
import apsara.odps.lot.StreamingTransformProtos.StreamingTransform;
import apsara.odps.lot.TableScanProtos;
import apsara.odps.lot.TableSinkProtos.TableSink;
import apsara.odps.lot.TransformProtos.Transform;
import apsara.odps.lot.UnionAllProtos.UnionAll;
import apsara.odps.lot.VolumeProtos.Volume;

public class LOTGenerator {

  private static final Log LOG = LogFactory.getLog(LOTGenerator.class);

  private static final String NO_OUTPUT_DUMMY_COLUMN = "__no_output__";
  private static final String MULTI_INSERT_SELECTOR = "__multiins_selector__";
  private static final String PARTITION_ID = "__partition_id__";
  private static final String MAP_OUT_KEY_PREFIX = "k_";
  private static final String MAP_OUT_VAL_PREFIX = "v_";
  private static final String INNER_OUTPUT_SELECTOR = "__inner_output_selector__";

  private final String project;
  private final BridgeJobConf job;
  private Pipeline pipeline;
  private boolean pipeMode; // use pipeline mapreduce
  private List<ResourceItem> resourceItems;

  private int opId = 0;

  private boolean isStreamingMap;
  private boolean isStreamingReduce;
  
  private boolean hasReducer;
  private boolean hasPartitioner;
  private boolean isMultiInsert;
  private boolean isNoOutput;
  private boolean isInnerOutput;
  private boolean isTableOverwrite;

  private TableInfo[] inputTableInfos;
  private VolumeInfo[] inputVolumeInfos;
  private TableInfo[] outputTableInfos;
  private VolumeInfo[] outputVolumeInfos;
  private List<Column> outputColumns = new ArrayList<Column>();
  private Map<String, Integer> outputIndexes = new HashMap<String, Integer>();

  public LOTGenerator(String project, JobConf job, Pipeline pipeline) {
    this.project = project;
    this.job = new BridgeJobConf(job);
    this.pipeline = pipeline;
    if (this.pipeline != null) {
      this.pipeMode = true;
    }

    hasReducer = this.pipeMode ? pipeline.getNodeNum() > 1 : job.getNumReduceTasks() > 0;
    hasPartitioner = this.pipeMode ? pipeline.getFirstNode().getPartitionerClass() != null : 
                     job.getPartitionerClass() != null;
    isStreamingMap = job.get("stream.map.streamprocessor", null) != null;
    isStreamingReduce = job.get("stream.reduce.streamprocessor", null) != null;
    isInnerOutput = job.getInnerOutputEnable();
    isTableOverwrite = isInnerOutput ? false : job.getOutputOverwrite();
  }

  public byte[] generate() {

    LotTask.Builder builder = LotTask.newBuilder();

    builder.setLot(genTree());

    LotTask lotTask = builder.build();
    LOG.debug(lotTask.toString());

    return lotTask.toByteArray();
  }

  LogicalOperatorTree genTree() {

    resourceItems = buildResourceList();

    LogicalOperatorTree.Builder builder = LogicalOperatorTree.newBuilder();

    // prepare input 
    inputTableInfos = InputUtils.getTables(job);
    inputVolumeInfos = InputUtils.getVolumes(job);
    // FIXME multi-mapper
    Map<TableInfoKey, List<LinkedHashMap<String, String>>>
        inputTables =
        mergeInputTableInfos(inputTableInfos);

    // prepare output
    outputTableInfos = OutputUtils.getTables(job);
    outputVolumeInfos = OutputUtils.getVolumes(job);

    // FIXME multi-insert from m-r's mapper
    isNoOutput = outputTableInfos == null;
    isMultiInsert = !isNoOutput && outputTableInfos.length > 1;

    // streaming job has string output columns
    boolean isStreamingOutput = job.getNumReduceTasks() > 0 ?
                                isStreamingReduce :
                                isStreamingMap;

    List<OdpsType> outputColumnTypes = new ArrayList<OdpsType>();
    if (isMultiInsert) {
      // concat output columns for multi-insert
      for (TableInfo ti : outputTableInfos) {
        List<OdpsType> tbColumnTypes = new ArrayList<OdpsType>();
        for (Column col : job.getOutputSchema(ti.getLabel())) {
          tbColumnTypes.add(col.getType());
        }
        // check if the same columns already exists
        int idx = Collections.indexOfSubList(outputColumnTypes, tbColumnTypes);
        if (idx >= 0) {
          // merge columns for tableinfos with the same schema
          outputIndexes.put(ti.getLabel(), idx);
          continue;
        }
        idx = outputColumns.size();
        outputIndexes.put(ti.getLabel(), idx);
        for (Column col : job.getOutputSchema(ti.getLabel())) {
          String colName = "multiins" + idx + "_" + col.getName();
          if (isStreamingOutput) {
            outputColumns.add(new Column(colName, OdpsType.STRING));
            outputColumnTypes.add(OdpsType.STRING);
          } else {
            outputColumns.add(TypeUtils.createColumnWithNewName(colName, col));
            outputColumnTypes.add(col.getType());
          }
        }
      }
      outputColumns.add(new Column(MULTI_INSERT_SELECTOR, OdpsType.STRING));
    } else if (isNoOutput) {
      // FIXME currently UDTF need a output column
      outputColumns.add(new Column(NO_OUTPUT_DUMMY_COLUMN, OdpsType.STRING));
    } else {
      for (Column col : job.getOutputSchema(outputTableInfos[0].getLabel())) {
        if (isStreamingOutput) {
          outputColumns.add(new Column(col.getName(), OdpsType.STRING));
        } else {
          outputColumns.add(TypeUtils.cloneColumn(col));
        }
      }
    }

    // prepare intermediate key/value
    // FIXME types/signature
    // FIXME use col name or not?
    List<Column> mapOutColumns;
    List<Column> firstReduceInColumns = null;
    int innerOutputIndex = 0;
    if (hasReducer) {
      mapOutColumns = new ArrayList<Column>();
      firstReduceInColumns = new ArrayList<Column>();

      if (hasPartitioner) {
        mapOutColumns.add(new Column(PARTITION_ID, OdpsType.BIGINT));
      }

      Column[] keys = this.pipeMode ?
                      pipeline.getFirstNode().getOutputKeySchema() :
                      job.getMapOutputKeySchema() ;
      for (Column col : keys) {
        Column keyCol = TypeUtils.createColumnWithNewName(MAP_OUT_KEY_PREFIX + col.getName(), col);
        mapOutColumns.add(keyCol);
        firstReduceInColumns.add(keyCol);
      }
      Column[] values = this.pipeMode ?
                        pipeline.getFirstNode().getOutputValueSchema() :
                        job.getMapOutputValueSchema();
      for (Column col : values) {
        Column valCol = TypeUtils.createColumnWithNewName(MAP_OUT_VAL_PREFIX + col.getName(), col);
        mapOutColumns.add(valCol);
        firstReduceInColumns.add(valCol);
      }
    } else {
      mapOutColumns = outputColumns;
    }

    //XXX: lot not support multi inputs with inner output
    String mapperId = genMapBlock(builder, inputTables, mapOutColumns, innerOutputIndex,
        hasReducer && isInnerOutput && (inputTables.size() <= 1));

    if (hasReducer) {
      genReduceBlock(builder, firstReduceInColumns, mapperId);
    } else {
      // map only output
      handleOutput(builder, false, outputColumns, mapperId, isTableOverwrite, innerOutputIndex);
    }

    return builder.build();
  }

  private int appendInnerOutputColumns(List<Column> mapOutColumns) {
    int innerOutputIndex;
    innerOutputIndex = mapOutColumns.size();
    for (Column col : outputColumns) {
      if (col.getName().equals(MULTI_INSERT_SELECTOR)) {
        // not to change the name of multi insert selector
        mapOutColumns.add(col);
        continue;
      }
      Column valCol =
          TypeUtils.createColumnWithNewName(
              "inneroutputs" + innerOutputIndex + "_" + col.getName(), col);
      mapOutColumns.add(valCol);
    }
    mapOutColumns.add(new Column(INNER_OUTPUT_SELECTOR, OdpsType.STRING));
    return innerOutputIndex;
  }

  private String genMapBlock(LogicalOperatorTree.Builder builder,
      Map<TableInfoKey, List<LinkedHashMap<String, String>>> inputTables,
      List<Column> mapOutColumns, int innerOutputIndex, boolean innerOutput) {
    if (innerOutput) {
      innerOutputIndex = appendInnerOutputColumns(mapOutColumns);
    }
    // XXX one mapper only process one table
    List<String> mappers = new ArrayList<String>();
    List<Column> mapCols = mapOutColumns;
    if (inputTables.size() == 0) {
      int numMapTasks = job.getNumMapTasks();
      if (isStreamingMap) {
        DataSource emptySource = genEmptyStreamingSource(builder, numMapTasks);
        String mapperId =
            genMapper(builder, emptySource.getId(), new Column[0], mapOutColumns,
                emptySource.getId());
        mappers.add(mapperId);
      } else {
        // no input, implement mapper as Java DataSource
        DataSource mapper = genJavaSource(builder, numMapTasks, mapOutColumns);
        String mapperId = mapper.getId();
        if (hasReducer && innerOutput) {
          mapCols = mapOutColumns.subList(0, innerOutputIndex);
          mapperId =
              this.genInnerOutputBlock(builder, mapOutColumns, innerOutputIndex, mapperId, mapperId);
        }
        mappers.add(mapperId);
      }
    } else {
      for (Map.Entry<TableInfoKey, List<LinkedHashMap<String, String>>> e : inputTables.entrySet()) {
        TableInfo inputTable = e.getKey().getTableInfo();
        List<LinkedHashMap<String, String>> partList = e.getValue();

        DataSource tableSource = genTableSource(builder, inputTable);

        String mapParentId = tableSource.getId();

        if (!partList.isEmpty()) {
          Filter partFilter =
              genPartitionFilter(builder, tableSource.getId(), partList, tableSource.getId());
          mapParentId = partFilter.getId();
        }

        String mapperId =
            genMapper(builder, tableSource.getId(), job.getInputSchema(inputTable), mapOutColumns,
                mapParentId);
        if (hasReducer && innerOutput) {
          mapCols = mapOutColumns.subList(0, innerOutputIndex);
          mapperId =
              this.genInnerOutputBlock(builder, mapOutColumns, innerOutputIndex, mapperId, mapperId);
        }
        mappers.add(mapperId);
      }
    }

    String mapperId;
    if (mappers.size() > 1) {
      UnionAll.Builder unionAllBuilder = UnionAll.newBuilder();
      for (String mid : mappers) {
        Select mapOutSelect = genSelect(builder, mid, mapCols, mid);
        unionAllBuilder.addParents(mapOutSelect.getId());
      }
      unionAllBuilder.setId("UNION_" + opId++);
      UnionAll mapper = unionAllBuilder.build();
      LogicalOperator.Builder b = LogicalOperator.newBuilder();
      b.setUnionAll(mapper);
      builder.addOperators(b.build());
      mapperId = mapper.getId();
    } else {
      mapperId = mappers.get(0);
    }
    return mapperId;
  }

  private String genReduceBlock(LogicalOperatorTree.Builder tree,
      List<Column> firstReduceInColumns, String finalId) {
    List<List<Column>> reduceInColumnsList = new ArrayList<List<Column>>();
    List<List<Column>> reduceOutColumnsList = new ArrayList<List<Column>>();

    reduceInColumnsList.add(firstReduceInColumns);

    int innerOutputIndex = 0;
    if (this.pipeMode) {
      for (int i = 1; i < pipeline.getNodeNum() - 1; i++) {
        List<Column> reduceOut = new ArrayList<Column>();
        List<Column> reduceIn = new ArrayList<Column>();
        if (pipeline.getNode(i).getPartitionerClass() != null) {
          reduceOut.add(new Column(PARTITION_ID, OdpsType.BIGINT));
        }
        for (Column col : pipeline.getNode(i).getOutputKeySchema()) {
          Column keyCol =
              TypeUtils.createColumnWithNewName(MAP_OUT_KEY_PREFIX + col.getName(), col);
          reduceOut.add(keyCol);
          reduceIn.add(keyCol);
        }
        for (Column col : pipeline.getNode(i).getOutputValueSchema()) {
          Column valCol =
              TypeUtils.createColumnWithNewName(MAP_OUT_VAL_PREFIX + col.getName(), col);
          reduceOut.add(valCol);
          reduceIn.add(valCol);
        }
        if (isInnerOutput) {
          innerOutputIndex = appendInnerOutputColumns(reduceOut);
        }
        reduceOutColumnsList.add(reduceOut);
        reduceInColumnsList.add(reduceIn);
      }
    }
    reduceOutColumnsList.add(outputColumns);

    // TODO combiner (currently run in mapper)
    TransformNode node = null;
    for (int i = 0; i < reduceInColumnsList.size(); i++) {
      List<Column> reduceInCols = reduceInColumnsList.get(i);
      List<Column> reduceOutCols = reduceOutColumnsList.get(i);
      String sourceId = finalId;
      String parentId = finalId;
      if (this.pipeMode) {
        // reduce block index from 0 to max
        // reducers in node index from 1 to max [Mapper, Reducer, Reducer, ...]
        // this reduce block's shuffle setting is set by previous map/reduce node
        node = pipeline.getNode(i + 1 - 1);
      }
      boolean hasPartitioner =
          this.pipeMode ? node.getPartitionerClass() != null : job.getPartitionerClass() != null;
      // partitioner
      String[] partitionColumns =
          hasPartitioner ? new String[] {PARTITION_ID} : transformKeyColumnNames(pipeMode ? node
              .getPartitionColumns() : job.getPartitionColumns());

      DistributeBy shuffle = genShuffle(tree, sourceId, Arrays.asList(partitionColumns), parentId);

      String[] sortColumns =
          transformKeyColumnNames(pipeMode ? node.getOutputKeySortColumns() : job
              .getOutputKeySortColumns());
      JobConf.SortOrder[] order =
          pipeMode ? node.getOutputKeySortOrder() : job.getOutputKeySortOrder();
      SortBy sort = genSort(tree, sourceId, sortColumns, order, shuffle.getId());

      // XXX group comparer only used inside reduce udtf, so no responding lot operator
      parentId = sort.getId();
      Transform reducer = genReducer(tree, sourceId, reduceInCols, reduceOutCols, parentId);
      sourceId = parentId = reducer.getId();

      // output
      if (i == reduceInColumnsList.size() - 1) {
        handleOutput(tree, false, outputColumns, sourceId, isTableOverwrite, 0);
      } else if (isInnerOutput) {
        finalId = genInnerOutputBlock(tree, reduceOutCols, innerOutputIndex, sourceId, parentId);
      } else {
        finalId = reducer.getId();
      }
    }
    return finalId;
  }

  private String genInnerOutputBlock(LogicalOperatorTree.Builder builder,
      List<Column> reduceOutCols, int innerOutputIndex, String sourceId, String parentId) {
    String finalId;
    handleOutput(builder, true, reduceOutCols, sourceId, false, innerOutputIndex);
    Filter innertOutputSelector =
        genInnerOutputSelector(builder, sourceId, parentId, TableInfo.DEFAULT_LABEL);
    parentId = innertOutputSelector.getId();
    Select reduceOutSelect =
        genSelect(builder, sourceId, reduceOutCols.subList(0, innerOutputIndex), parentId);
    finalId = reduceOutSelect.getId();
    return finalId;
  }

  private String handleOutput(LogicalOperatorTree.Builder tree, boolean innerOutput,
      List<Column> outputColumns, String finalTaskId,
      boolean overwrite, int innerOutputIndex) {
    String parentId = null;
    if (isMultiInsert) {
      for (TableInfo ti : outputTableInfos) {
        // multi-insert filter
        Filter multiInsertSelector =
            genMultiInsertSelector(tree, finalTaskId, ti.getLabel(), finalTaskId);
        parentId = multiInsertSelector.getId();
        if (innerOutput) {
          Filter innertOutputSelector =
              genInnerOutputSelector(tree, finalTaskId, parentId, TableInfo.INNER_OUTPUT_LABEL);
          parentId = innertOutputSelector.getId();
        }
        int idx = innerOutputIndex + outputIndexes.get(ti.getLabel());
        List<Column> columns =
            outputColumns.subList(idx, idx + job.getOutputSchema(ti.getLabel()).length);
        Select outputSelect = genSelect(tree, finalTaskId, columns, parentId);
        parentId = outputSelect.getId();
        genTableSink(tree, ti, parentId, overwrite);
      }
    } else if (isNoOutput) {
      genFakeSink(tree, finalTaskId);
    } else {
      List<Column> columns = outputColumns;
      parentId = finalTaskId;
      if (innerOutput) {
        Filter innertOutputSelector =
            genInnerOutputSelector(tree, finalTaskId, finalTaskId, TableInfo.INNER_OUTPUT_LABEL);
        parentId = innertOutputSelector.getId();
        columns = outputColumns.subList(innerOutputIndex, outputColumns.size() - 1);
      }
      Select outputSelect = genSelect(tree, finalTaskId, columns, parentId);
      parentId = outputSelect.getId();
      genTableSink(tree, outputTableInfos[0], parentId, overwrite);
    }

    return parentId;
  }

  private DataSource genTableSource(LogicalOperatorTree.Builder tree, TableInfo tableInfo) {

    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    DataSource.Builder db = DataSource.newBuilder();

    TableScanProtos.TableScan.Builder tb = apsara.odps.lot.TableScanProtos.TableScan
        .newBuilder();
    tb.setTable(tableInfo.getTableName());
    tb.setProject(tableInfo.getProjectName() == null ? project : tableInfo.getProjectName());

    db.setTableScan(tb.build());

    db.setId("DataSource_" + opId++);
    DataSource dataSource = db.build();

    builder.setDataSource(dataSource);
    tree.addOperators(builder.build());
    return dataSource;
  }

  private Filter genPartitionFilter(LogicalOperatorTree.Builder tree, String sourceId,
                                    List<LinkedHashMap<String, String>> partList, String parentId) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();
    Filter.Builder fb = Filter.newBuilder();

    List<ScalarExpression> parts = new ArrayList<ScalarExpression>();

    for (LinkedHashMap<String, String> partSpec : partList) {
      ScalarExpression lastCol = null;
      for (Map.Entry<String, String> e : partSpec.entrySet()) {
        ScalarExpression.Builder colBuilder = ScalarExpression.newBuilder();

        ScalarFunction.Builder eqBuilder = ScalarFunction.newBuilder();
        eqBuilder.setProject(project);
        eqBuilder.setName("EQ");

        ScalarExpression.Builder keyBuilder = ScalarExpression.newBuilder();
        Reference.Builder keyReference = Reference.newBuilder();
        keyReference.setName(e.getKey());
        keyReference.setFrom(sourceId);
        keyBuilder.setReference(keyReference.build());
        eqBuilder.addParameters(keyBuilder.build());

        ScalarExpression.Builder valueBuilder = ScalarExpression.newBuilder();
        Constant.Builder valueConstant = Constant.newBuilder();
        valueConstant.setString(e.getValue());
        valueBuilder.setConstant(valueConstant.build());
        eqBuilder.addParameters(valueBuilder.build());

        colBuilder.setExpression(eqBuilder.build());

        if (lastCol == null) {
          lastCol = colBuilder.build();
        } else {
          ScalarExpression.Builder newColBuilder = ScalarExpression.newBuilder();
          ScalarFunction.Builder andBuilder = ScalarFunction.newBuilder();
          andBuilder.setProject(project);
          andBuilder.setName("AND");
          andBuilder.addParameters(lastCol);
          andBuilder.addParameters(colBuilder.build());
          newColBuilder.setExpression(andBuilder.build());
          lastCol = newColBuilder.build();
        }
      }
      parts.add(lastCol);
    }

    // generate condition expression as binary tree, to limit nesting depth
    List<ScalarExpression> children = parts;
    while (children.size() > 1) {
      List<ScalarExpression> parents = new ArrayList<ScalarExpression>(children.size() / 2 + 1);
      for (int i = 0; i < children.size(); i += 2) {
        ScalarExpression eLeft = children.get(i);
        if (i + 1 >= children.size()) {
          parents.add(eLeft);
        } else {
          ScalarExpression eRight = children.get(i + 1);
          ScalarExpression.Builder parentBuilder = ScalarExpression.newBuilder();
          ScalarFunction.Builder orBuilder = ScalarFunction.newBuilder();
          orBuilder.setProject(project);
          orBuilder.setName("OR");
          orBuilder.addParameters(eLeft);
          orBuilder.addParameters(eRight);
          parentBuilder.setExpression(orBuilder.build());
          parents.add(parentBuilder.build());
        }
      }
      children = parents;
    }
    ScalarExpression partCond = children.get(0);

    fb.setCondition(partCond);
    fb.setParentId(parentId);
    fb.setId("FIL_" + opId++);

    Filter filter = fb.build();

    builder.setFilter(filter);
    tree.addOperators(builder.build());
    return filter;
  }

  private String genMapper(LogicalOperatorTree.Builder tree, String sourceId, Column[] inColumns,
                           List<Column> outColumns, String parentId) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    boolean isStreaming = isStreamingMap;

    Transform.Builder ab = Transform.newBuilder();
    for (ResourceItem item : resourceItems) {
      Transform.Resources.Builder rb = Transform.Resources.newBuilder();
      rb.setProject(item.projectName);
      rb.setResourceName(item.resourceName);
      ab.addResources(rb.build());
    }

    if (!isStreaming) {
      LanguageTransform.Builder tb = LanguageTransform.newBuilder();
      tb.setClassName(LotMapperUDTF.class.getName());
      tb.setLanguage(Language.Java);

      ab.setLanguageTransform(tb.build());
    } else {
      StreamingTransform.Builder sb = StreamingTransform.newBuilder();
      sb.setCmd(job.get("stream.map.streamprocessor", null));
      // TODO properties to pb fields
      fillStreamingMapProperties(sb);
      ab.setStreamingTransform(sb.build());
    }

    for (Column col : inColumns) {
      ScalarExpression.Builder exprBuilder = ScalarExpression.newBuilder();

      Reference.Builder refBuilder = Reference.newBuilder();
      refBuilder.setName(col.getName());
      refBuilder.setFrom(sourceId);

      if (isStreaming) {
        if (col.getType().equals(OdpsType.BOOLEAN)) {
          exprBuilder.setExpression(castBooleanAsStreamingString(refBuilder.build()));
        } else if (!col.getType().equals(OdpsType.STRING)) {
          // cast as string
          ScalarFunction.Builder castBuilder = ScalarFunction.newBuilder();
          castBuilder.setProject(project);
          castBuilder.setName("TOSTRING");
          castBuilder.addParameters(ScalarExpression.newBuilder().setReference(refBuilder.build()).build());
          exprBuilder.setExpression(castBuilder.build());
        }
        else {
          exprBuilder.setReference(refBuilder.build());
        }
      } else {
        exprBuilder.setReference(refBuilder.build());
      }

      ab.addParameters(exprBuilder.build());
    }

    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (Column col : outColumns) {
      Schema.Columns.Builder scb = Schema.Columns.newBuilder();
      scb.setName(col.getName());
      scb.setType((isStreaming && !col.getName().equals(PARTITION_ID)) ? TypesProtos.Type.String
              : TypeUtils.getLotTypeFromColumn(col));
      schemaBuilder.addColumns(scb.build());
    }
    ab.setSchema(schemaBuilder.build());

    ab.setParentId(parentId);
    ab.setId("MapTransform_" + opId++);

    //volume related
    if (inputVolumeInfos != null && inputVolumeInfos.length > 0) {
      for (VolumeInfo vol : inputVolumeInfos) {
        Volume.Builder volumeBuilder = Volume.newBuilder();
        volumeBuilder.setProject(vol.getProjectName());
        volumeBuilder.setVolumeName(vol.getVolumeName());
        volumeBuilder.setPartition(vol.getPartSpec());
        volumeBuilder.setLabel(vol.getLabel());
        volumeBuilder.setIsInput(true);
        ab.addVolumes(volumeBuilder.build());
      }
    }
    if (outputVolumeInfos != null && outputVolumeInfos.length > 0) {
      for (VolumeInfo vol : outputVolumeInfos) {
        Volume.Builder volumeBuilder = Volume.newBuilder();
        volumeBuilder.setProject(vol.getProjectName());
        volumeBuilder.setVolumeName(vol.getVolumeName());
        volumeBuilder.setPartition(vol.getPartSpec());
        volumeBuilder.setLabel(vol.getLabel());
        volumeBuilder.setIsInput(false);
        ab.addVolumes(volumeBuilder.build());
      }
    }

    Transform mapper = ab.build();

    builder.setTransform(mapper);
    tree.addOperators(builder.build());

    String mapperId = mapper.getId();

    if (job.getNumReduceTasks() > 0 && isStreaming) {
      // convert key type for shuffle keys
      boolean hasNonStringOutput = false;
      for (Column col : outColumns) {
        if (!col.getName().equals(PARTITION_ID) && !col.getType().equals(OdpsType.STRING)) {
          hasNonStringOutput = true;
        }
      }
      if (hasNonStringOutput) {
        // add select for converting from string
        Select.Builder sb = Select.newBuilder();

        for (Column col : outColumns) {
          Select.Expressions.Builder seb = Select.Expressions.newBuilder();
          ScalarExpression.Builder eb = ScalarExpression.newBuilder();

          Reference.Builder refBuilder = Reference.newBuilder();
          refBuilder.setName(col.getName());
          refBuilder.setFrom(mapper.getId());

          if (col.getName().equals(PARTITION_ID) || col.getType().equals(OdpsType.STRING)) {
            eb.setReference(refBuilder.build());
          } else {
            // cast from string
            ScalarFunction.Builder castBuilder = ScalarFunction.newBuilder();
            castBuilder.setProject(project);
            castBuilder.setName("TO" + col.getType().toString());
            castBuilder.addParameters(
                ScalarExpression.newBuilder().setReference(refBuilder.build()).build());
            eb.setExpression(castBuilder.build());
          }

          seb.setExpression(eb.build());
          seb.setAlias(col.getName());
          sb.addExpressions(seb.build());
        }

        sb.setParentId(mapper.getId());
        sb.setId("SEL_" + opId++);

        Select select = sb.build();

        tree.addOperators(LogicalOperator.newBuilder().setSelect(select).build());

        mapperId = select.getId();
      }
    }

    return mapperId;
  }

  private DataSource genJavaSource(LogicalOperatorTree.Builder tree, int instanceCount,
                                   List<Column> outColumns) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    DataSource.Builder db = DataSource.newBuilder();
    LanguageSource.Builder jb = LanguageSource.newBuilder();
    jb.setClassName(LotMapperUDTF.class.getName());
    jb.setLanguage(Language.Java);

    for (ResourceItem item : resourceItems) {
      LanguageSource.Resources.Builder rb = LanguageSource.Resources.newBuilder();
      rb.setProject(item.projectName);
      rb.setResourceName(item.resourceName);
      jb.addResources(rb.build());
    }

    jb.setInstanceCount(instanceCount);

    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (Column col : outColumns) {
      Schema.Columns.Builder scb = Schema.Columns.newBuilder();
      scb.setName(col.getName());
      scb.setType(TypeUtils.getLotTypeFromColumn(col));
      schemaBuilder.addColumns(scb.build());
    }
    jb.setSchema(schemaBuilder.build());

    //volume related
    if (inputVolumeInfos != null && inputVolumeInfos.length > 0) {
      for (VolumeInfo vol : inputVolumeInfos) {
        Volume.Builder volumeBuilder = Volume.newBuilder();
        volumeBuilder.setProject(vol.getProjectName());
        volumeBuilder.setVolumeName(vol.getVolumeName());
        volumeBuilder.setPartition(vol.getPartSpec());
        volumeBuilder.setLabel(vol.getLabel());
        volumeBuilder.setIsInput(true);
        jb.addVolumes(volumeBuilder.build());
      }
    }
    if (outputVolumeInfos != null && outputVolumeInfos.length > 0) {
      for (VolumeInfo vol : outputVolumeInfos) {
        Volume.Builder volumeBuilder = Volume.newBuilder();
        volumeBuilder.setProject(vol.getProjectName());
        volumeBuilder.setVolumeName(vol.getVolumeName());
        volumeBuilder.setPartition(vol.getPartSpec());
        volumeBuilder.setLabel(vol.getLabel());
        volumeBuilder.setIsInput(false);
        jb.addVolumes(volumeBuilder.build());
      }
    }
    db.setLanguageSource(jb.build());

    db.setId("MapJavaSource_" + opId++);

    DataSource mapper = db.build();

    builder.setDataSource(mapper);
    tree.addOperators(builder.build());

    return mapper;
  }

  // HACK for no-input streaming
  private DataSource genEmptyStreamingSource(LogicalOperatorTree.Builder tree, int instanceCount) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    DataSource.Builder db = DataSource.newBuilder();
    LanguageSource.Builder jb = LanguageSource.newBuilder();
    jb.setClassName(EmptyDataSource.class.getName());
    jb.setLanguage(Language.Java);

    jb.setInstanceCount(instanceCount);

    jb.setSchema(Schema.newBuilder().build());

    db.setLanguageSource(jb.build());

    db.setId("EmptySource_" + opId++);

    DataSource emptySource = db.build();
    builder.setDataSource(emptySource);
    tree.addOperators(builder.build());

    return emptySource;
  }

  private Transform genReducer(LogicalOperatorTree.Builder tree, String sourceId,
                               List<Column> mapOutColumns, List<Column> outputColumns,
                               String parentId) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    Transform.Builder ab = Transform.newBuilder();

    boolean isStreaming = isStreamingReduce;

    for (ResourceItem item : resourceItems) {
      Transform.Resources.Builder rb = Transform.Resources.newBuilder();
      rb.setProject(item.projectName);
      rb.setResourceName(item.resourceName);
      ab.addResources(rb.build());
    }

    if (!isStreaming) {
      LanguageTransform.Builder tb = LanguageTransform.newBuilder();
      tb.setClassName(LotReducerUDTF.class.getName());
      tb.setLanguage(Language.Java);

      ab.setLanguageTransform(tb.build());
    } else {
      StreamingTransform.Builder sb = StreamingTransform.newBuilder();
      sb.setCmd(job.get("stream.reduce.streamprocessor", null));
      fillStreamingReduceProperties(sb);
      ab.setStreamingTransform(sb.build());
    }

    for (Column col : mapOutColumns) {
      ScalarExpression.Builder exprBuilder = ScalarExpression.newBuilder();

      Reference.Builder refBuilder = Reference.newBuilder();
      refBuilder.setName(col.getName());
      refBuilder.setFrom(sourceId);
      if (isStreaming && !col.getType().equals(OdpsType.STRING)) {
        // cast as string
        ScalarFunction.Builder castBuilder = ScalarFunction.newBuilder();
        castBuilder.setProject(project);
        castBuilder.setName("TOSTRING");
        castBuilder
            .addParameters(ScalarExpression.newBuilder().setReference(refBuilder.build()).build());
        exprBuilder.setExpression(castBuilder.build());
      } else {
        exprBuilder.setReference(refBuilder.build());
      }

      ab.addParameters(exprBuilder.build());
    }

    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (Column col : outputColumns) {
      Schema.Columns.Builder scb = Schema.Columns.newBuilder();
      scb.setName(col.getName());
      scb.setType(TypeUtils.getLotTypeFromColumn(col));
      schemaBuilder.addColumns(scb.build());
    }
    ab.setSchema(schemaBuilder.build());

    ab.setParentId(parentId);
    ab.setId("ReduceTransform_" + opId++);

    Transform reducer = ab.build();

    builder.setTransform(reducer);
    tree.addOperators(builder.build());

    return reducer;
  }

  private DistributeBy genShuffle(LogicalOperatorTree.Builder tree, String sourceId,
                                  List<String> columns, String parentId) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    DistributeBy.Builder db = DistributeBy.newBuilder();

    for (String col : columns) {
      Reference.Builder refBuilder = Reference.newBuilder();
      refBuilder.setName(col);
      refBuilder.setFrom(sourceId);
      db.addColumns(refBuilder.build());
    }

    db.setParentId(parentId);
    db.setId("DIS_" + opId++);

    DistributeBy shuffle = db.build();

    builder.setDistributeBy(shuffle);
    tree.addOperators(builder.build());
    return shuffle;
  }

  private SortBy genSort(LogicalOperatorTree.Builder tree, String sourceId, String[] sortColumns,
                         JobConf.SortOrder[] order, String parentId) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    SortBy.Builder sb = SortBy.newBuilder();

    sb.setIsPartial(false);

    assert sortColumns.length == order.length;
    for (int i = 0; i < sortColumns.length; i++) {
      Order.Builder o = Order.newBuilder();
      Reference.Builder refBuilder = Reference.newBuilder();
      refBuilder.setName(sortColumns[i]);
      refBuilder.setFrom(sourceId);
      o.setColumn(refBuilder.build());
      o.setAsc(order[i] == JobConf.SortOrder.ASC);

      sb.addOrders(o.build());
    }

    sb.setParentId(parentId);
    sb.setId("SORT_" + opId++);

    SortBy sort = sb.build();

    builder.setSortBy(sort);
    tree.addOperators(builder.build());
    return sort;
  }

  private Select genSelect(LogicalOperatorTree.Builder tree, String sourceId, List<Column> columns,
                           String parentId) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();
    Select.Builder sb = Select.newBuilder();

    for (Column col : columns) {
      Select.Expressions.Builder seb = Select.Expressions.newBuilder();
      ScalarExpression.Builder eb = ScalarExpression.newBuilder();

      Reference.Builder refBuilder = Reference.newBuilder();
      refBuilder.setName(col.getName());
      refBuilder.setFrom(sourceId);
      eb.setReference(refBuilder.build());

      seb.setExpression(eb.build());
      seb.setAlias(col.getName());
      sb.addExpressions(seb.build());
    }

    sb.setParentId(parentId);
    sb.setId("SEL_" + opId++);

    Select select = sb.build();

    builder.setSelect(sb.build());
    tree.addOperators(builder.build());
    return select;
  }

  private DataSink genTableSink(LogicalOperatorTree.Builder tree, TableInfo tableInfo, String parentId, boolean overwrite) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    DataSink.Builder db = DataSink.newBuilder();

    TableSink.Builder tw = TableSink.newBuilder();
    tw.setProject(tableInfo.getProjectName() == null ? project : tableInfo.getProjectName());
    tw.setTable(tableInfo.getTableName());
    tw.setIsOverwrite(overwrite);

    LinkedHashMap<String, String> partSpec = tableInfo.getPartSpec();
    if (!partSpec.isEmpty()) {
      PartitionSpec.Builder pb = PartitionSpec.newBuilder();
      for (Map.Entry<String, String> e : partSpec.entrySet()) {
        PartitionSpec.Items.Builder ib = PartitionSpec.Items.newBuilder();
        ib.setKey(e.getKey());
        Constant.Builder cb = Constant.newBuilder();
        cb.setString(e.getValue());
        ib.setValue(cb.build());
        pb.addItems(ib.build());
      }
      tw.setPartition(pb.build());
    }

    db.setTableSink(tw.build());

    db.setParentId(parentId);
    db.setId("DataSink_" + opId++);

    DataSink dataSink = db.build();
    builder.setDataSink(dataSink);
    
    tree.addOperators(builder.build());

    return dataSink;
  }

  private Filter genMultiInsertSelector(LogicalOperatorTree.Builder tree, String sourceId,
                                        String label, String parentId) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();
    Filter.Builder fb = Filter.newBuilder();

    ScalarExpression.Builder condBuilder = ScalarExpression.newBuilder();
    ScalarFunction.Builder opBuilder = ScalarFunction.newBuilder();
    opBuilder.setProject(project);
    opBuilder.setName("EQ");

    {
      ScalarExpression.Builder eb = ScalarExpression.newBuilder();

      Reference.Builder refBuilder = Reference.newBuilder();
      refBuilder.setName(MULTI_INSERT_SELECTOR);
      refBuilder.setFrom(sourceId);
      eb.setReference(refBuilder.build());
      opBuilder.addParameters(eb.build());
    }
    {
      ScalarExpression.Builder eb = ScalarExpression.newBuilder();

      Constant.Builder cBuilder = Constant.newBuilder();
      cBuilder.setString(label);
      eb.setConstant(cBuilder.build());
      opBuilder.addParameters(eb.build());
    }

    condBuilder.setExpression(opBuilder.build());
    fb.setCondition(condBuilder.build());
    fb.setParentId(parentId);
    fb.setId("FIL_" + opId++);

    Filter selector = fb.build();

    builder.setFilter(selector);
    tree.addOperators(builder.build());

    return selector;
  }

  private Filter genInnerOutputSelector(LogicalOperatorTree.Builder tree, String sourceId, String parentId, String label) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();
    Filter.Builder fb = Filter.newBuilder();

    ScalarExpression.Builder condBuilder = ScalarExpression.newBuilder();
    ScalarFunction.Builder opBuilder = ScalarFunction.newBuilder();
    opBuilder.setProject(project);
    opBuilder.setName("EQ");

    {
      ScalarExpression.Builder eb = ScalarExpression.newBuilder();

      Reference.Builder refBuilder = Reference.newBuilder();
      refBuilder.setName(INNER_OUTPUT_SELECTOR);
      refBuilder.setFrom(sourceId);
      eb.setReference(refBuilder.build());
      opBuilder.addParameters(eb.build());
    }
    {
      ScalarExpression.Builder eb = ScalarExpression.newBuilder();

      Constant.Builder cBuilder = Constant.newBuilder();
      cBuilder.setString(label);
      eb.setConstant(cBuilder.build());
      opBuilder.addParameters(eb.build());
    }

    condBuilder.setExpression(opBuilder.build());
    fb.setCondition(condBuilder.build());
    fb.setParentId(parentId);
    fb.setId("FIL_" + opId++);

    Filter selector = fb.build();

    builder.setFilter(selector);
    tree.addOperators(builder.build());

    return selector;
  }

  private DataSink genFakeSink(LogicalOperatorTree.Builder tree, String parentId) {
    LogicalOperator.Builder builder = LogicalOperator.newBuilder();

    DataSink.Builder db = DataSink.newBuilder();

    db.setFakeSink(FakeSinkProtos.FakeSink.newBuilder().build());

    db.setParentId(parentId);
    db.setId("DataSink_" + opId++);

    DataSink dataSink = db.build();
    builder.setDataSink(dataSink);

    tree.addOperators(builder.build());

    return dataSink;
  }

  private static class TableInfoKey {

    private TableInfo tableInfo;

    public TableInfoKey(TableInfo tableInfo) {
      this.tableInfo = tableInfo;
    }

    public TableInfo getTableInfo() {
      return tableInfo;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof TableInfoKey)) {
        return false;
      }
      TableInfoKey other = (TableInfoKey) o;
      String prj = this.tableInfo.getProjectName();
      String otherPrj = other.tableInfo.getProjectName();
      // XXX default project issue
      if (StringUtils.equals(prj, otherPrj)
          && StringUtils.equals(this.tableInfo.getTableName(), other.tableInfo.getTableName())) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      int code = this.tableInfo.getTableName().hashCode();
      String prj = this.tableInfo.getProjectName();
      if (prj != null) {
        code = code * 71 + prj.hashCode();
      }
      return code;
    }
  }

  /**
   * Group table infos by table name, and gather partitions.
   * XXX non-partitioned table will have empty partition list
   * XXX one mapper should only process one table?
   * XXX one table/partition should only be processed by one mapper?
   */
  private Map<TableInfoKey, List<LinkedHashMap<String, String>>> mergeInputTableInfos(
      TableInfo[] inputTableInfos) {
    Map<TableInfoKey, List<LinkedHashMap<String, String>>>
        inputTables =
        new HashMap<TableInfoKey, List<LinkedHashMap<String, String>>>();
    if (inputTableInfos == null) {
      return inputTables;
    }
    for (TableInfo ti : inputTableInfos) {
      LinkedHashMap<String, String> partSpec = null;
      if (ti.getPartSpec() != null && !ti.getPartSpec().isEmpty()) {
        partSpec = ti.getPartSpec();
      }
      TableInfoKey key = new TableInfoKey(ti);
      List<LinkedHashMap<String, String>> partList = inputTables.get(key);
      if (partList == null) {
        // new table
        partList = new ArrayList<LinkedHashMap<String, String>>();
        if (partSpec != null) {
          partList.add(partSpec);
        }
        inputTables.put(key, partList);
      } else {
        // detect conflict
        if (partList.isEmpty()) {
          if (partSpec != null) {
            throw new IllegalArgumentException("conflict input for table:" + ti.getTableName());
          } else {
            throw new IllegalArgumentException("duplicate input for table:" + ti.getTableName());
          }
        }
        if (partSpec == null) {
          throw new IllegalArgumentException("conflict input for table:" + ti.getTableName());
        }
        partList.add(partSpec);
      }
    }
    return inputTables;
  }

  private static class ResourceItem {

    public String projectName;
    public String resourceName;
    public String linkName;

    public ResourceItem(String projectName, String resourceName, String linkName) {
      this.projectName = projectName;
      this.resourceName = resourceName;
      this.linkName = linkName;
    }
  }

  private List<ResourceItem> buildResourceList() {
    List<ResourceItem> r = new ArrayList<ResourceItem>();
    if (job.getResources() == null) {
      return r;
    }
    for (String res : job.getResources()) {
      // FIXME parse resource
      String resProject;
      String resName;
      String linkName = null;
      String[] parts = res.split("/");
      if (parts.length == 1) {
        resProject = this.project;
        resName = parts[0];
      } else if (parts.length == 3 && parts[1].equals("resources")) {
        resProject = parts[0];
        resName = parts[2];
      } else {
        throw new IllegalArgumentException("Invalid resource name: '" + res + "'");
      }
      String[] nameParts = resName.split("#");
      if (nameParts.length == 1) {
        // normal
      } else if (nameParts.length == 2) {
        // resName#alias
        resName = nameParts[0];
        linkName = nameParts[1];
      } else {
        throw new IllegalArgumentException("Invalid resource name: '" + resName + "'");
      }

      // TODO merge these resource aliases

      r.add(new ResourceItem(resProject, resName, linkName));
    }
    return r;
  }

  private String[] transformKeyColumnNames(String[] cols) {
    String[] keyCols = new String[cols.length];
    for (int i = 0; i < cols.length; i++) {
      keyCols[i] = MAP_OUT_KEY_PREFIX + cols[i];
    }
    return keyCols;
  }

  private void fillStreamingMapProperties(StreamingTransform.Builder sb) {
    // TODO set per table streaming properties?
    for (Map.Entry<String, String> e : job) {
      addStreamingProperty(sb, e.getKey(), e.getValue());
    }
    addStreamingProperty(sb, "stream.stage", "map");
  }

  private void fillStreamingReduceProperties(StreamingTransform.Builder sb) {
    for (Map.Entry<String, String> e : job) {
      addStreamingProperty(sb, e.getKey(), e.getValue());
    }
    addStreamingProperty(sb, "stream.stage", "reduce");
  }

  private void addStreamingProperty(StreamingTransform.Builder sb, String name, String value) {
    sb.addProperties(
        StreamingTransform.Properties.newBuilder().setKey(name).setValue(value).build());
  }

  // WHEN(EQ(col, true), "true", WHEN(EQ(col, false), "false", NULL))
  private ScalarFunction castBooleanAsStreamingString(Reference colRef)
  {
    ScalarFunction.Builder caseBuilder = ScalarFunction.newBuilder();
    caseBuilder.setProject(project);
    caseBuilder.setName("WHEN");

    ScalarFunction.Builder eqBuilder = ScalarFunction.newBuilder();
    eqBuilder.setProject(project);
    eqBuilder.setName("EQ");
    eqBuilder.addParameters(ScalarExpression.newBuilder().setReference(colRef).build());
    eqBuilder.addParameters(ScalarExpression.newBuilder().setConstant(Constant.newBuilder().setBool(true).build()).build());
    
    caseBuilder.addParameters(ScalarExpression.newBuilder().setExpression(eqBuilder.build()).build());
    caseBuilder.addParameters(ScalarExpression.newBuilder().setConstant(Constant.newBuilder().setString("true").build()).build());

    ScalarFunction.Builder caseBuilder2 = ScalarFunction.newBuilder();
    caseBuilder2.setProject(project);
    caseBuilder2.setName("WHEN");

    ScalarFunction.Builder eqBuilder2 = ScalarFunction.newBuilder();
    eqBuilder2.setProject(project);
    eqBuilder2.setName("EQ");
    eqBuilder2.addParameters(ScalarExpression.newBuilder().setReference(colRef).build());
    eqBuilder2.addParameters(ScalarExpression.newBuilder().setConstant(Constant.newBuilder().setBool(false).build()).build());

    caseBuilder2.addParameters(ScalarExpression.newBuilder().setExpression(eqBuilder2.build()).build());
    caseBuilder2.addParameters(ScalarExpression.newBuilder().setConstant(Constant.newBuilder().setString("false").build()).build());
    caseBuilder2.addParameters(ScalarExpression.newBuilder().setNull(Null.newBuilder().build()).build());
    
    caseBuilder.addParameters(ScalarExpression.newBuilder().setExpression(caseBuilder2.build()).build());

    return caseBuilder.build();
  }

}
