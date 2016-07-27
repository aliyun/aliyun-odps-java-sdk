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

package com.aliyun.odps.graph.local.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.Aggregator;
import com.aliyun.odps.graph.DataType;
import com.aliyun.odps.graph.DefaultLoadingVertexResolver;
import com.aliyun.odps.graph.GRAPH_CONF;
import com.aliyun.odps.graph.JobConf;
import com.aliyun.odps.graph.Partitioner;
import com.aliyun.odps.graph.VertexResolver;
import com.aliyun.odps.graph.local.HashPartitioner;
import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.DatetimeWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.ExceptionCode;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.utils.CommonUtils;
import com.aliyun.odps.utils.ReflectionUtils;
import com.aliyun.odps.utils.StringUtils;

import sun.misc.BASE64Decoder;

public class LocalGraphRunUtils {

  private static final Log LOG = LogFactory.getLog(LocalGraphRunUtils.class);

  private final static BASE64Decoder baseDecoder = new BASE64Decoder();
  private final static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");

  public static String generateGraphTaskName() {
    return "console_graph_" + LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_1)
        .format(new Date());
  }

  public static String generateLocalGraphTaskName() {
    return "graph_" + LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_1).format(new Date()) + "_"
           + LocalRunUtils.getPID();
  }

  public static int getMaxGraphTasks() {
    String s = System.getProperty("odps.graph.local.max.workers");
    int maxGraphWorkers = 0;
    if (!StringUtils.isEmpty(s)) {
      maxGraphWorkers = Integer.parseInt(s);
      CommonUtils.checkArgument("max graph workers", maxGraphWorkers, 0, 100);
    }
    return 100;
  }

  @SuppressWarnings("rawtypes")
  public static Partitioner createPartitioner(JobConf conf) throws IOException {

    Partitioner p = null;
    Class<? extends Partitioner> partitionClass = conf.getPartitionerClass();
    if (partitionClass != null) {
      p = ReflectionUtils.newInstance(partitionClass, conf);
    } else {
      p = new HashPartitioner();
    }
    p.configure(conf);
    return p;

  }

  @SuppressWarnings("rawtypes")
  public static List<Aggregator> getAggregator(JobConf conf) {
    String classes = conf.get(GRAPH_CONF.AGGREGATOR_CLASSES);
    try {
      List<Aggregator> ret = new ArrayList<Aggregator>();
      if (classes != null) {
        String[] classNames = classes.split(";");
        for (String className : classNames) {
          if (!StringUtils.isEmpty(className)) {
            Class c = Class.forName(className);
            Aggregator aggr = (Aggregator) c.newInstance();
            ret.add(aggr);
          }
        }
      }
      return ret;
    } catch (Exception e) {
      System.err
          .println(com.aliyun.odps.utils.StringUtils.stringifyException(e));
      List<Aggregator> ret = new ArrayList<Aggregator>();
      return ret;
    }
  }

  @SuppressWarnings("rawtypes")
  public static VertexResolver createLoadingVertexResolver(JobConf conf)
      throws IOException {
    Class<? extends VertexResolver> cls = conf.getLoadingVertexResolverClass();
    if (cls == null) {
      cls = DefaultLoadingVertexResolver.class;
    }
    VertexResolver resolver = ReflectionUtils.newInstance(cls, conf);
    resolver.configure(conf);
    return resolver;
  }

  @SuppressWarnings("rawtypes")
  public static VertexResolver createSuperstepVertexResolver(JobConf conf) throws IOException {
    Class<? extends VertexResolver> cls = conf
        .getComputingVertexResolverClass();
    if (cls == null) {
      return null;
    }
    VertexResolver resolver = ReflectionUtils.newInstance(cls, conf);
    resolver.configure(conf);
    return resolver;
  }

  public static JobConf getGraphJobConf(JobConf conf) {
    return new JobConf((Configuration) conf);
  }

  public static JobConf getJobConf(JobConf conf) {
    return new JobConf(new com.aliyun.odps.mapred.conf.JobConf(conf));
  }

  public static Writable fromString(byte type, String val, String nullIndicator)
      throws IOException {
    if (!nullIndicator.equals(val)) {
      switch (type) {
        case DataType.INTEGER:
          return new LongWritable(Long.parseLong(val));
        case DataType.STRING:
          if (val.startsWith("ODPS-BASE64")) {
            return new Text(baseDecoder.decodeBuffer(val.substring("ODPS-BASE64".length())));
          } else {
            try {
              byte[] v = LocalRunUtils.fromReadableString(val);
              return new Text(v);
            } catch (Exception e) {
              throw new RuntimeException("from readable string failed!" + e);
            }
          }
        case DataType.DOUBLE:
          return new DoubleWritable(Double.parseDouble(val));
        case DataType.BOOLEAN:
          return new BooleanWritable(Boolean.parseBoolean(val));
        case DataType.DATETIME:
          return parseDateTime(val);
        default:
          throw new IOException("unsupported type: " + type);
      }
    } else {
      return null;
    }
  }

  private static Writable parseDateTime(String val) throws IOException {
    try {
      return new DatetimeWritable(Long.parseLong(val));
    } catch (NumberFormatException e) {
      try {
        return new DatetimeWritable(DATETIME_FORMAT.parse(val).getTime());
      } catch (ParseException ex) {
        throw new IOException("unsupported date time format:" + val);
      }
    }
  }

  private static TableInfo[] getTables(JobConf conf, String descKey) throws IOException {
    String inputDesc = conf.get(descKey, "[]");
    if (inputDesc != "[]") {
      JSONArray inputs = JSON.parseArray(inputDesc);
      TableInfo[] infos = new TableInfo[inputs.size()];
      for (int i = 0; i < inputs.size(); i++) {
        JSONObject input = inputs.getJSONObject(i);
        String projName = input.getString("projName");
        if (StringUtils.isEmpty(projName)) {
          projName = SessionState.get().getOdps().getDefaultProject();
        }
        String tblName = input.getString("tblName");
        if (StringUtils.isEmpty(tblName)) {
          throw new IOException(ExceptionCode.ODPS_0720001
                                + " - input table name cann't be empty: " + input);
        }

        JSONArray parts = input.getJSONArray("partSpec");
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
        for (int j = 0; j < parts.size(); j++) {
          String part = parts.getString(j);
          String[] part_val = part.split("=");
          partSpec.put(part_val[0], part_val[1]);
        }
        String[] cols = null;
        if (input.get("cols") != null) {
          String readCols = input.getString("cols");

          // check column duplicate
          cols = readCols.split("\\,");
          for (int cur = 0; cur < cols.length; ++cur) {
            for (int k = cur + 1; k < cols.length; ++k) {
              if (cols[cur].equals(cols[k])) {
                throw new IOException(ExceptionCode.ODPS_0720091 + " - "
                                      + cols[cur]);
              }
            }
          }
        }
        String label = TableInfo.DEFAULT_LABEL;
        if (input.get("label") != null) {
          String tmpLabel = input.getString("label");
          if (!StringUtils.isEmpty(tmpLabel)) {
            label = tmpLabel;
          }
        }
        TableInfo info = TableInfo.builder().tableName(tblName).projectName(projName)
            .partSpec(partSpec).cols(cols).label(label).build();
        infos[i] = info;
      }
      return infos;
    }
    return new TableInfo[0];
  }

  public static TableInfo[] getInputTables(JobConf conf) throws IOException {
    return getTables(conf, GRAPH_CONF.INPUT_DESC);
  }

  public static TableInfo[] getOutputTables(JobConf conf) throws IOException {
    return getTables(conf, GRAPH_CONF.OUTPUT_DESC);
  }

  public static Writable[] createFileds(byte[] types) {
    Writable[] fields = new Writable[types.length];
    for (int i = 0; i < fields.length; i++) {
      switch (types[i]) {
        case DataType.INTEGER:
          fields[i] = new LongWritable();
          break;
        case DataType.DOUBLE:
          fields[i] = new DoubleWritable();
          break;
        case DataType.BOOLEAN:
          fields[i] = new BooleanWritable();
          break;
        case DataType.STRING:
          fields[i] = new Text();
          break;
        case DataType.DATETIME:
          fields[i] = new DatetimeWritable();
          break;

        default:
          throw new RuntimeException("Unsupported column type: " + types[i]);
      }
    }
    return fields;
  }

}