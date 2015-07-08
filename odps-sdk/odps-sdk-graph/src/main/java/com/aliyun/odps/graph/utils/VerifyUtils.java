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

package com.aliyun.odps.graph.utils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import com.aliyun.odps.graph.Combiner;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.JobConf;
import com.aliyun.odps.graph.Partitioner;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerComputer;
import com.aliyun.odps.graph.common.COMMON_GRAPH_CONF;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableComparable;
import com.aliyun.odps.utils.ReflectionUtils;

/**
 * 用于验证提交的Graph作业合法性验证的工具类
 */
@SuppressWarnings("rawtypes")
public class VerifyUtils {

  private static Class<? extends Vertex> vertexClass;
  private static Class<? extends WritableComparable> vertexIdClass;
  private static Class<? extends Writable> vertexValueClass;
  private static Class<? extends Writable> edgeValueClass;
  private static Class<? extends Writable> messageValueClass;

  private static String udfPropertiesClassName = "com.aliyun.odps.graph.udf.UDFPropertiesImpl";

  private static void verifyPartitioner(JobConf jobConf, Type vertexIdType) {
    Class<? extends Partitioner> partitionerClass = jobConf
        .getPartitionerClass();
    if (partitionerClass != null) {
      List<Class<?>> partitionerClassList = ReflectionUtils
          .<Partitioner>getTypeArguments(Partitioner.class, partitionerClass);

      if (!partitionerClassList.get(0).equals(vertexIdType)) {
        throw new RuntimeException(
            "ODPS-0730001: Vertex id generic type of partitioner class and vertex class not match,"
            + " former is "
            + ReflectionUtils.getClass(partitionerClassList.get(0))
            + ", the later is " + ReflectionUtils.getClass(vertexIdType));
      }
    } else {
      String partitionerClassStr = jobConf.get(COMMON_GRAPH_CONF.PARTITIONER_CLASS);
      if (partitionerClassStr != null) {
        throw new RuntimeException("ODPS-0730001: Partition class "
                                   + partitionerClassStr + " not found");
      }
    }

  }

  private static void verifyCombiner(JobConf jobConf, Type vertexIdType,
                                     Type messageValueType) {
    Class<? extends Combiner> combinerClass = jobConf.getCombinerClass();
    if (combinerClass != null) {
      List<Class<?>> combinerClassList = ReflectionUtils
          .<Combiner>getTypeArguments(Combiner.class, combinerClass);
      if (combinerClassList.size() != 2) {
        throw new RuntimeException("ODPS-0730001: combiner " + combinerClass
                                   + " should have 2 generic types for vertex id and message value");
      }
      if (!combinerClassList.get(0).equals(vertexIdType)) {
        throw new RuntimeException(
            "ODPS-0730001: Vertex id generic type of combiner class and vertex class not match,"
            + " former is "
            + ReflectionUtils
                .getClass(combinerClassList.get(0))
            + ", the later is "
            + ReflectionUtils.getClass(vertexIdType));
      } else if (!combinerClassList.get(1).equals(messageValueType)) {
        throw new RuntimeException(
            "ODPS-0730001: Message value generic type of combiner class and vertex class not match,"
            + " former is "
            + ReflectionUtils
                .getClass(combinerClassList.get(1))
            + ", the later is "
            + ReflectionUtils
                .getClass(messageValueType));
      }
    } else {
      String combinerClassStr = jobConf.get(COMMON_GRAPH_CONF.COMBINER_CLASS);
      if (combinerClassStr != null) {
        throw new RuntimeException("ODPS-0730001: Combiner class "
                                   + combinerClassStr + " not found");
      }
    }
  }

  /**
   * 验证作业配置conf是否合法
   *
   * @param jobConf
   *     作业配置conf
   */
  @SuppressWarnings({"unchecked"})
  public static void verifyGraphConf(JobConf jobConf) {
    vertexClass = jobConf.getVertexClass();
    if (vertexClass == null) {
      throw new RuntimeException("ODPS-0730001: Vertex class not set");
    }
    Class<? extends GraphLoader> loaderClass = jobConf.getGraphLoaderClass();
    if (loaderClass == null) {
      throw new RuntimeException("ODPS-0730001: GraphLoader class not set");
    }

    List<Class<?>> vertexTypeClassList = ReflectionUtils
        .<Vertex>getTypeArguments(Vertex.class, vertexClass);

    List<Class<?>> graphLoaderTypeClassList = ReflectionUtils
        .<GraphLoader>getTypeArguments(GraphLoader.class, loaderClass);

    Type vertexIdType = vertexTypeClassList.get(0);
    Type vertexValueType = vertexTypeClassList.get(1);
    Type edgeValueType = vertexTypeClassList.get(2);
    Type messageValueType = vertexTypeClassList.get(3);

    vertexIdClass = (Class<? extends WritableComparable>) ReflectionUtils
        .getClass(vertexIdType);
    vertexValueClass = (Class<? extends Writable>) ReflectionUtils
        .getClass(vertexValueType);
    edgeValueClass = (Class<? extends Writable>) ReflectionUtils
        .getClass(edgeValueType);
    messageValueClass = (Class<? extends Writable>) ReflectionUtils
        .getClass(messageValueType);

    String
        format =
        "ODPS-0730001: %s generic type of GraphLoader class and vertex class not match, former is %s, the later is %s.";
    String errMsg = null;
    if (!graphLoaderTypeClassList.get(0).equals(vertexIdType)) {
      errMsg = String.format(format, "Vertex id",
                             ReflectionUtils.getClass(graphLoaderTypeClassList.get(0)),
                             vertexIdClass);
    } else if (!graphLoaderTypeClassList.get(1).equals(vertexValueType)) {
      errMsg = String.format(format, "Vertex value",
                             ReflectionUtils.getClass(graphLoaderTypeClassList.get(1)),
                             vertexValueClass);
    } else if (!graphLoaderTypeClassList.get(2).equals(edgeValueType)) {
      errMsg = String.format(format, "Edge value",
                             ReflectionUtils.getClass(graphLoaderTypeClassList.get(2)),
                             edgeValueClass);
    } else if (!graphLoaderTypeClassList.get(3).equals(messageValueType)) {
      errMsg = String.format(format, "Message value",
                             ReflectionUtils.getClass(graphLoaderTypeClassList.get(3)),
                             messageValueClass);
    }
    if (errMsg != null) {
      throw new RuntimeException(errMsg);
    }

    verifyPartitioner(jobConf, vertexIdType);
    verifyCombiner(jobConf, vertexIdType, messageValueType);

    // check vertex id implements has hashCode and equals method
    ReflectionUtils.findDeclaredMethod(vertexIdClass, "hashCode");
    ReflectionUtils.findDeclaredMethod(vertexIdClass, "equals");

    /** check vertex implementation has non-static field */
    /** interactive job will skip the check */
    if (!vertexClass.getName().equals("com.aliyun.odps.graph.udf.UDFVertexImpl")) {
      ReflectionUtils.checkNonStaticField(vertexClass, Vertex.class);
    }
    /**
     * check worker computer implementation has non-static field, Default has no
     * field
     */
    Class<?> workerComputerClass = jobConf.getWorkerComputerClass();
    ReflectionUtils.checkNonStaticField(workerComputerClass,
                                        WorkerComputer.class);

    /** put vertex template argument to job configuration */
    jobConf.setClass(COMMON_GRAPH_CONF.VERTEX_ID_CLASS, vertexIdClass,
                     WritableComparable.class);
    jobConf.setClass(COMMON_GRAPH_CONF.VERTEX_VALUE_CLASS, vertexValueClass,
                     Writable.class);
    jobConf.setClass(COMMON_GRAPH_CONF.EDGE_VALUE_CLASS, edgeValueClass,
                     Writable.class);
    jobConf.setClass(COMMON_GRAPH_CONF.MESSAGE_VALUE_CLASS, messageValueClass,
                     Writable.class);
  }

  /**
   * 验证作业ID是否合法
   *
   * @param id
   *     作业ID
   * @throws IOException
   */
  public static void verifyVertexId(WritableComparable id) throws IOException {
    if (id == null) {
      throw new IOException("ODPS-0730001: Vertex id is null");
    }

    if (id.getClass() != vertexIdClass) {
      throw new IOException("ODPS-0730001: Vertex id type error, expect '"
                            + vertexIdClass + "', but '" + id.getClass() + "'");
    }

  }

  /**
   * 验证顶点Value是否合法
   *
   * @param value
   *     顶点Value
   * @throws IOException
   */
  public static void verifyVertexValue(Writable value) throws IOException {
    if (value != null) {
      String propertiesClassName = "com.aliyun.odps.graph.udf.UDFPropertiesImpl";
      if (!(value.getClass() == vertexValueClass ||
            value.getClass().getName().equals(udfPropertiesClassName))) {
        throw new IOException(
            "ODPS-0730001: Vertex value type of error, expect '"
            + vertexValueClass + "', but '" + value.getClass() + "'");
      }
    }
  }

  /**
   * 验证边是否合法
   *
   * @param edge
   *     边
   * @throws IOException
   */
  public static void verifyVertexEdge(Edge edge) throws IOException {
    if (edge == null) {
      throw new IOException("ODPS-0730001: Edge is null");
    }
    WritableComparable destVertexId = edge.getDestVertexId();
    if (destVertexId == null) {
      throw new IOException("ODPS-0730001: DestVertexId of " + edge.toString()
                            + " is null");
    }
    if (destVertexId.getClass() != vertexIdClass) {
      throw new IOException("ODPS-0730001: Dest vertex id type of " + edge
                            + " error, expect '" + vertexIdClass + "', but '"
                            + destVertexId.getClass() + "'");
    }

    Writable edgeValue = edge.getValue();
    if (edgeValue != null) {
      if (!(edgeValue.getClass() == edgeValueClass ||
            edgeValue.getClass().getName().equals(udfPropertiesClassName))) {
        throw new IOException("ODPS-0730001: Edge value type of " + edge
                              + " error, expect '" + edgeValueClass + "', but '"
                              + edgeValue.getClass() + "'");
      }
    }
  }

  /**
   * 验证顶点是否合法
   *
   * @param vertex
   *     顶点
   * @throws IOException
   */
  @SuppressWarnings({"unchecked"})
  public static void verifyVertex(Vertex vertex) throws IOException {
    if (vertex == null) {
      throw new IOException("ODPS-0730001: Vertex is null");
    }

    if (vertex.getClass() != vertexClass) {
      throw new IOException("ODPS-0730001: Vertex type of " + vertex
                            + " error, expect '" + vertexClass + "', but '" + vertex.getClass()
                            + "'");
    }

    try {
      verifyVertexId(vertex.getId());
      verifyVertexValue(vertex.getValue());

      // edges can be null                                                                                                                                     
      if (vertex.hasEdges()) {
        for (Edge<WritableComparable, Writable> edge : (List<Edge<WritableComparable, Writable>>) vertex
            .getEdges()) {
          verifyVertexEdge(edge);
        }
      }
    } catch (IOException e) {
      throw new IOException(e.getMessage() + ", from " + vertex.toString());
    }

  }

}
