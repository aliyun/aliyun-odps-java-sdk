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

package com.aliyun.odps.mapred.conf;

import com.aliyun.odps.Column;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.RecordComparator;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class BridgeJobConf extends JobConf {

  public BridgeJobConf(boolean loadSessionContext) {
    super(loadSessionContext);
  }

  public BridgeJobConf(Configuration conf) {
    super(conf);
  }

  public BridgeJobConf() {
    super();
  }

  /**
   * 获取输入行属性.
   *
   * @param table
   *     输入表信息
   * @return 行属性
   */
  public Column[] getInputSchema(TableInfo table) {
    return SchemaUtils.fromString(get(CONF.INPUT_SCHEMA + "."
                                      + table.getProjectName().toLowerCase() + "." + table
        .getTableName().toLowerCase()));
  }

  /**
   * 设置输入行属性.
   *
   * @param table
   *     输入表信息
   * @param schema
   *     行属性
   */
  public void setInputSchema(TableInfo table, Column[] schema) {
    set(CONF.INPUT_SCHEMA + "." + table.getProjectName().toLowerCase() + "."
        + table.getTableName().toLowerCase(), SchemaUtils.toString(schema));
  }

  /**
   * 获取输出行属性.
   *
   * @return 行属性
   */
  public Column[] getOutputSchema() {
    return SchemaUtils.fromString(get(CONF.OUTPUT_SCHEMA + "." + TableInfo.DEFAULT_LABEL));
  }

  /**
   * 多路输出时获取指定label表的输出行属性
   *
   * @param label
   *     输出标签
   * @return 输出行属性
   */
  public Column[] getOutputSchema(String label) {
    String schema = get(CONF.OUTPUT_SCHEMA + "." + label);
    assert schema != null;
    return SchemaUtils.fromString(schema);
  }

  /**
   * 设置指定label的输出行属性
   *
   * @param schema
   *     输出行属性
   * @param label
   *     输出标签
   */
  public void setOutputSchema(Column[] schema, String label) {
    set(CONF.OUTPUT_SCHEMA + "." + label, SchemaUtils.toString(schema));
  }

  /**
   * interfaces bellow is hotfix, to remove it in sprint23
   */
  private final static String COMBINER_CACHE_SPILL_PERCENT = "odps.mapred.combiner.cache.spill.percent";
  public float getCombinerCacheSpillPercent() {
    return getFloat(COMBINER_CACHE_SPILL_PERCENT, (float)0.5);
  }

  private final static String COMBINER_OPTIMIZE_ENABLE = "odps.mapred.combiner.optimize.enable";
  public boolean getCombinerOptimizeEnable() {
    return getBoolean(COMBINER_OPTIMIZE_ENABLE, false);
  }

  private final static String OUTPUT_KEY_COMPARATOR_CLASS = "odps.stage.mapred.output.key.comparator.class";
  public Class<? extends RecordComparator> getOutputKeyComparatorClass() {
    return getClass(OUTPUT_KEY_COMPARATOR_CLASS, null, RecordComparator.class);
  }

  private final static String OUTPUT_KEY_GROUPING_COMPARATOR_CLASS = "odps.mapred.output.key.grouping.comparator.class";
  public Class<? extends RecordComparator> getOutputKeyGroupingComparatorClass() {
    return getClass(OUTPUT_KEY_GROUPING_COMPARATOR_CLASS, null, RecordComparator.class);
  }

  private final static String PIPELINE = "odps.pipeline.";
  private final static String KEY_COMPARATOR_CLASS = ".output.key.comparator.class";
  private final static String KEY_GROUPING_COMPARATOR_CLASS = ".output.key.grouping.comparator.class";
  public Class<? extends RecordComparator> getPipelineOutputKeyComparatorClass(int idx) {
    return getClass(PIPELINE + idx + KEY_COMPARATOR_CLASS, null, RecordComparator.class);
  }
  public Class<? extends RecordComparator> getPipelineOutputKeyGroupingComparatorClass(int idx) {
    return getClass(PIPELINE + idx + KEY_GROUPING_COMPARATOR_CLASS, null, RecordComparator.class);
  }

  public final static String INNER_OUTPUT_ENABLE = "odps.mapred.inner.output.enable";
  public boolean getInnerOutputEnable() {
    return getBoolean(INNER_OUTPUT_ENABLE, false);
  }
  public final static String MAPPER_INNER_OUTPUT_ENABLE = "odps.mapred.mapper.inner.output.enable";
  public void setMapperInnerOutputEnable(boolean mapperInnerOutputEnable) {
    setBoolean(MAPPER_INNER_OUTPUT_ENABLE, mapperInnerOutputEnable);
  }
  public boolean getMapperInnerOutputEnable() {
    return getBoolean(MAPPER_INNER_OUTPUT_ENABLE, true);
  }
  public final static String OUTPUT_OVERWRITE = "odps.mapred.output.overwrite";
  public boolean getOutputOverwrite() {
    return getBoolean(OUTPUT_OVERWRITE, true);
  }
}
