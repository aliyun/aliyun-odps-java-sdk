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

package com.aliyun.odps.pipeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.RecordComparator;
import com.aliyun.odps.io.WritableComparator;
import com.aliyun.odps.mapred.Job;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.JobConf.SortOrder;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * MapReduce Pipeline扩展<br/>
 * <p>
 * 在传统的MapReduce计算模型上做了扩展，可以在一轮Map/Reduce运算之后再加上一个或多个Reduce，
 * 即Map-Reduce-Reduce-Reduce...。一个Pipeline由多个节点顺序组成，每个节点是Mapper或Reducer<br/><br/>
 * 和普通的MapReduce一样，使用{@link Job}定义并提交作业，包括输入输入表的设置等。
 * 除此之外，还需要定义一个Pipeline对象（使用Pipeline.builder()方法），添加一个Mapper以及一个
 * 或多个Reducer。
 * <br/><br/>
 * 任何一个Mapper或处于中间状态的Reducer都需要显式定义输出结果的Key和Value的Schema定义
 * （定义方式类似于{@link JobConf#setMapOutputKeySchema(Column[])}）。同时还可以设置
 * OutputKeySortColumns、PartitionColumns等。
 * <br/><br/>
 * 代码示例如下：
 * </p>
 *
 * <pre>
 * Job job = new Job();
 *
 * Pipeline pipeline = Pipeline.builder()
 *                  .addMapper(TokenizerMapper.class)
 *                  .setOutputKeySchema(
 *                      new Column[] { new Column("word", OdpsType.STRING) })
 *                  .setOutputValueSchema(
 *                      new Column[] { new Column("count", OdpsType.BIGINT) })
 *                  .addReducer(SumReducer.class)
 *                  .setOutputKeySchema(
 *                      new Column[] { new Column("count", OdpsType.BIGINT) })
 *                  .setOutputValueSchema(
 *                      new Column[] { new Column("word", OdpsType.STRING),
 *                      new Column("count", OdpsType.BIGINT) })
 *                  .addReducer(IdentityReducer.class).createPipeline();
 *
 * job.setPipeline(pipeline);
 * job.addInput(...)
 * job.addOutput(...)
 * job.submit();
 * </pre>
 */
public class Pipeline {

  private final static String PIPELINE_LIST = "odps.pipeline.list";
  private final static String PIPELINE = "odps.pipeline.";
  private final static String OUTPUT_KEY_SCHEMA = ".output.key.schema";
  private final static String OUTPUT_VALUE_SCHEMA = ".output.value.schema";
  private final static String OUTPUT_KEY_SORT_COLUMNS = ".output.key.sort.columns";
  private final static String OUTPUT_KEY_SORT_ORDER = ".output.key.sort.order";
  private final static String OUTPUT_GROUP_COLUMNS = ".output.group.columns";
  private final static String PARTITION_COLUMNS = ".partition.columns";
  private final static String PARTITION_CLASS = ".partition.class";
  private final static String OUTPUT_KEY_COMPARATOR_CLASS = ".output.key.comparator.class";
  private final static String OUTPUT_KEY_GROUPING_COMPARATOR_CLASS = ".output.key.grouping.comparator.class";

  private List<TransformNode> nodes = new ArrayList<TransformNode>();

  /**
   * Pipeline的节点，是一个Mapper或者Reducer
   */
  public abstract static class TransformNode {

    Column[] keySchema;
    Column[] valueSchema;
    String[] sortCols;
    SortOrder[] order;
    String[] partCols;
    Class<? extends Partitioner> partitionerClass;
    @SuppressWarnings("rawtypes")
    Class<? extends RecordComparator> keyComparatorClass;
    @SuppressWarnings("rawtypes")
    Class<? extends RecordComparator> keyGroupingComparatorClass;
    String[] groupCols;
    String type;
    TransformNode prevNode;
    TransformNode nextNode;
    int taskNum = -1;
    int taskMemoryMB = -1;
    int jvmMemoryMB = -1;

    /**
     * 获取Pipeline节点的类型
     *
     * @return 字符串，map 或者 reduce
     */
    public String getType() {
      return this.type;
    }

    /**
     * 设置Pipeline上的前一个节点
     *
     * @param prev
     *     前一个节点
     */
    public void setPreviousNode(TransformNode prev) {
      this.prevNode = prev;
    }

    /**
     * 获取Pipeline上的前一个节点
     *
     * @return 前一个节点
     */
    public TransformNode getPreviousNode() {
      return this.prevNode;
    }

    /**
     * 设置Pipeline上的后一个节点
     *
     * @param next
     *     后一个节点
     */
    public void setNextNode(TransformNode next) {
      this.nextNode = next;
    }

    /**
     * 获取Pipeline上的后一个节点
     *
     * @return 后一个节点
     */
    public TransformNode getNextNode() {
      return this.nextNode;
    }

    /**
     * 获取本节点的输入数据Key Schema
     *
     * @return 输入数据的Key Schema
     */
    public Column[] getInputKeySchema() {
      if (this.prevNode != null) {
        return this.prevNode.getOutputKeySchema();
      } else {
        return null;
      }
    }

    /**
     * 获取本节点的输入数据Value Schema
     *
     * @return 输入数据的Value Schema，如果是Pipeline的第一个节点，则返回null
     */
    public Column[] getInputValueSchema() {
      if (this.prevNode != null) {
        return this.prevNode.getOutputValueSchema();
      } else {
        return null;
      }
    }

    /**
     * 获取本节点输入的Grouping列，也就是前一节点输出的Grouping列
     *
     * @return Grouping列，如果是Pipeline的第一个节点，则返回null
     */
    public String[] getInputGroupingColumns() {
      if (this.prevNode != null) {
        return this.prevNode.getOutputGroupingColumns();
      } else {
        return null;
      }
    }

    /**
     * 设置本节点的输出Key格式
     *
     * @param keySchema
     *     Key格式
     */
    public void setOutputKeySchema(Column[] keySchema) {
      this.keySchema = keySchema;
    }

    /**
     * 获取本节点的输出Key格式
     *
     * @return Key Schema列定义
     */
    public Column[] getOutputKeySchema() {
      return this.keySchema;
    }

    /**
     * 设置本节点的输出Value格式
     *
     * @param valueSchema
     *     Value格式
     */
    public void setOutputValueSchema(Column[] valueSchema) {
      this.valueSchema = valueSchema;
    }

    /**
     * 获取本节点的输出Value格式
     *
     * @return Value Schema列定义
     */
    public Column[] getOutputValueSchema() {
      return this.valueSchema;
    }

    /**
     * 设置输出按Key的排序方式
     *
     * @param order
     *     排序方式，是升序还是降序
     */
    public void setOutputKeySortOrder(SortOrder[] order) {
      this.order = order;
    }

    /**
     * 获取输出按Key的排序方式
     *
     * @return Key排序方式，是升序还是降序
     */
    public SortOrder[] getOutputKeySortOrder() {
      SortOrder[] order = this.order;
      if ((order == null || order.length == 0) &&
          this.getOutputKeySchema() != null) {
        order = new SortOrder[this.getOutputKeySchema().length];
        Arrays.fill(order, SortOrder.ASC);
      } else if (order == null) {
        order = new SortOrder[0];
      }
      return order;
    }

    /**
     * 设置输出排序的列
     *
     * @param sortCols
     *     排序列
     */
    public void setOutputKeySortColumns(String[] sortCols) {
      this.sortCols = sortCols;
    }

    /**
     * 获取输出排序的列
     *
     * @return 输出排序的列，如果没有显式指定，则按照输出Key排序
     */
    public String[] getOutputKeySortColumns() {
      if (this.sortCols != null) {
        return this.sortCols;
      } else if (this.keySchema != null) {
        return SchemaUtils.getNames(getOutputKeySchema());
      } else {
        return null;
      }
    }

    /**
     * 设置输出Partition的列
     *
     * @param partCols
     *     Partition列
     */
    public void setPartitionColumns(String[] partCols) {
      this.partCols = partCols;
    }

    /**
     * 获取输出数据切分列
     *
     * @return 输出数据切分列，如果没有显式指定，则按照输出Key做切分
     */
    public String[] getPartitionColumns() {
      if (this.partCols != null) {
        return this.partCols;
      } else if (this.keySchema != null) {
        return SchemaUtils.getNames(getOutputKeySchema());
      } else {
        return null;
      }
    }

    /**
     * 设置自定义的数据切分的class
     *
     * @param theClass
     *     切分的class
     */
    public void setPartitionerClass(Class<? extends Partitioner> theClass) {
      this.partitionerClass = theClass;
    }

    /**
     * 获取自定义的切分class
     *
     * @return 切分class
     */
    public Class<? extends Partitioner> getPartitionerClass() {
      return this.partitionerClass;
    }

    /**
     * 设置输出分组的列，如果不指定，默认按照输出Key分组
     *
     * @param groupCols
     *     分组列
     */
    public void setOutputGroupingColumns(String[] groupCols) {
      this.groupCols = groupCols;
    }

    /**
     * 获取输出分组的列
     *
     * @return 分组列，如果不显式指定，默认返回输出Key
     */
    public String[] getOutputGroupingColumns() {
      if (this.groupCols != null) {
        return this.groupCols;
      } else if (this.keySchema != null) {
        return SchemaUtils.getNames(getOutputKeySchema());
      } else {
        return null;
      }
    }

    /**
     * 获取本节点输入 Key 的排序比较器，默认返回上级节点的#getOutputKeyComparatorClass,
     * 如果没有上级节点则返回null.
     * 
     * @see RecordComparator
     * @return 输入 Key 排序比较器.
     * 
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends RecordComparator> getInputKeyComparatorClass() {
      if (this.getPreviousNode() != null) {
        return this.getPreviousNode().getOutputKeyComparatorClass();
      }
      
      return null;
    }

    /**
     * 获取本节点输入 Key 的分组比较器，默认返回上级节点的#getOutputKeyGroupingComparatorClass,
     * 如果没有上级节点则返回null.
     * 
     * @see RecordComparator
     * @return 输入 Key 的分组比较器.
     * 
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends RecordComparator> getInputKeyGroupingComparatorClass() {
      if (this.getPreviousNode() != null) {
        return this.getPreviousNode().getOutputKeyGroupingComparatorClass();
      }
      
      return null;
    }

    /**
     * 获取指定的节点输出 Key 排序比较器，如果没有指定，默认使用
     * {@link WritableComparator#get(Class)} 返回的比较函数.
     * 
     * @see RecordComparator
     * @return 输出 Key 排序比较器.
     * 
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends RecordComparator> getOutputKeyComparatorClass() {
      return keyComparatorClass;
    }

    /**
     * 设置输出 Key 排序比较器.
     * 
     * @param theClass
     *          用于输出 Key 排序的比较器，{@link RecordComparator} 子类
     * 
     */
    @SuppressWarnings("rawtypes")
    public void setOutputKeyComparatorClass(Class<? extends RecordComparator> theClass) {
      keyComparatorClass = theClass;
    }

    /**
     * 获取指定的 Key 分组比较器，默认为 {@link #getOutputKeyComparatorClass()}.
     * 
     * @see #setOutputKeyGroupingComparatorClass(Class)
     * @return Key 分组比较器
     * 
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends RecordComparator> getOutputKeyGroupingComparatorClass() {
      return keyGroupingComparatorClass;
    }

    /**
     * 设置 Key 分组比较器，如果不指定，默认使用 {@link #getOutputKeyComparatorClass()} 作为分组比较器.
     * 
     * @param theClass
     *          Key 分组比较器，实现 {@link RecordComparator}接口
     * @see #setOutputKeyComparatorClass(Class)
     * 
     */
    @SuppressWarnings("rawtypes")
    public void setOutputKeyGroupingComparatorClass(Class<? extends RecordComparator> theClass) {
      keyGroupingComparatorClass = theClass;
    }

    /**
     * 获取节点对应的Mapper或Reducer class定义
     *
     * @return
     */
    @SuppressWarnings("rawtypes")
    public abstract Class getTransformClass();

    /**
     * 设置当前节点任务数
     *
     * @param n
     *     任务数
     */
    public void setNumTasks(int n) {
      this.taskNum = n;
    }
    
    public int getNumTasks() {
      return this.taskNum;
    }
    
    /**
     * 设置当前节点的内存资源大小，单位：MB，默认值：2048.
     *
     * @param mem
     *     内存大小
     */
    public void setMemoryForTask(int mem) {
      this.taskMemoryMB = mem;
    }
    
    public int getMemoryForTask() {
      return this.taskMemoryMB;
    }
    
    /**
     * 设置当前节点的JVM虚拟机的内存资源大小，单位：MB，默认值：1024.
     *
     * @param mem
     *     内存大小
     */
    public void setMemoryForJVM(int mem) {
      this.jvmMemoryMB = mem;
    }
    
    public int getMemoryForJVM() {
      return this.jvmMemoryMB;
    }
  }

  /**
   * Mapper节点
   */
  public static class MapNode extends TransformNode {

    private Class<? extends Mapper> mapper;

    /**
     * 定义一个Mapper节点
     *
     * @param mapper
     *     class定义
     */
    public MapNode(Class<? extends Mapper> mapper) {
      this.mapper = mapper;
      this.type = "map";
    }

    @Override
    public Class<? extends Mapper> getTransformClass() {
      return this.mapper;
    }
  }

  /**
   * Reducer节点
   */
  public static class ReduceNode extends TransformNode {

    private Class<? extends Reducer> reducer;

    /**
     * 定义一个Reducer节点
     *
     * @param reducer
     *     class定义
     */
    public ReduceNode(Class<? extends Reducer> reducer) {
      this.reducer = reducer;
      this.type = "reduce";
    }

    @Override
    public Class<? extends Reducer> getTransformClass() {
      return this.reducer;
    }
  }

  /**
   * 定义一个Pipeline对象
   *
   * @param nodes
   *     Pipeline节点列表
   */
  public Pipeline(List<TransformNode> nodes) {
    this.nodes = nodes;
  }

  /**
   * 获取Pipeline中的指定节点
   *
   * @param index
   *     节点序号（从0开始）
   * @return Pipeline节点
   */
  public TransformNode getNode(int index) {
    if (index >= 0 && index < nodes.size()) {
      return this.nodes.get(index);
    } else {
      return null;
    }
  }

  /**
   * 获取Pipeline的第一个节点
   *
   * @return 第一个节点对象
   */
  public TransformNode getFirstNode() {
    return getNode(0);
  }

  /**
   * 获取Pipeline的最后一个节点
   *
   * @return 最后一个节点对象
   */
  public TransformNode getLastNode() {
    return getNode(getNodeNum() - 1);
  }

  /**
   * 获取Pipeline的节点数
   *
   * @return 节点数目
   */
  public int getNodeNum() {
    return this.nodes.size();
  }

  /**
   * 获取Pipeline的节点列表
   *
   * @return 节点列表
   */
  public List<TransformNode> getNodes() {
    return this.nodes;
  }

  /**
   * Pipeline Builder对象，可以用它来创建一个Pipeline
   */
  public static class Builder {

    private List<TransformNode> nodes = new ArrayList<TransformNode>();

    private TransformNode lastNode;

    /**
     * Pipeline增加一个Mapper节点
     *
     * @param mapper
     *     Mapper class定义
     * @return Builder对象
     */
    public Builder addMapper(Class<? extends Mapper> mapper) {
      // TODO check the previous node 
      //  if map after map, map after reduce can merge together
      MapNode map = new MapNode(mapper);

      this.nodes.add(map);
      this.lastNode = map;
      return this;
    }

    /**
     * Pipeline增加一个Mapper节点，同时定义该节点的输出格式等信息
     *
     * @param mapper
     *     mapper class定义
     * @param keySchema
     *     输出Key定义
     * @param valueSchema
     *     输出Value定义
     * @param sortCols
     *     输出排序列
     * @param order
     *     输出排序方式
     * @param partCols
     *     输出切分列
     * @param theClass
     *     输出自定义的切分类
     * @param groupCols
     *     输出分组列
     * @return Builder对象
     */
    public Builder addMapper(Class<? extends Mapper> mapper,
                             Column[] keySchema, Column[] valueSchema, String[] sortCols,
                             SortOrder[] order, String[] partCols,
                             Class<? extends Partitioner> theClass, String[] groupCols) {
      addMapper(mapper).setOutputKeySchema(keySchema)
          .setOutputValueSchema(valueSchema)
          .setOutputKeySortColumns(sortCols)
          .setOutputKeySortOrder(order)
          .setPartitionColumns(partCols)
          .setPartitionerClass(theClass)
          .setOutputGroupingColumns(groupCols);

      return this;
    }

    /**
     * Pipeline增加一个Reducer节点
     *
     * @param reducer
     *     Reducer class定义
     * @return Builder对象
     */
    public Builder addReducer(Class<? extends Reducer> reducer) {
      ReduceNode reduce = new ReduceNode(reducer);
      reduce.setPreviousNode(this.lastNode);
      if (lastNode != null) {
        lastNode.setNextNode(reduce);
      }

      this.nodes.add(reduce);
      this.lastNode = reduce;
      return this;
    }

    /**
     * Pipeline增加一个Reducer节点，同时定义该节点的输出格式等信息
     *
     * @param reducer
     *     Reducer class定义
     * @param keySchema
     *     输出Key定义
     * @param valueSchema
     *     输出Value定义
     * @param sortCols
     *     输出排序列
     * @param order
     *     输出排序方式
     * @param partCols
     *     输出切分列
     * @param theClass
     *     输出自定义的切分类
     * @param groupCols
     *     输出分组列
     * @return Builder对象
     */
    public Builder addReducer(Class<? extends Reducer> reducer,
                              Column[] keySchema, Column[] valueSchema, String[] sortCols,
                              SortOrder[] order, String[] partCols,
                              Class<? extends Partitioner> theClass, String[] groupCols) {
      addReducer(reducer).setOutputKeySchema(keySchema)
          .setOutputValueSchema(valueSchema)
          .setOutputKeySortColumns(sortCols)
          .setOutputKeySortOrder(order)
          .setPartitionColumns(partCols)
          .setPartitionerClass(theClass)
          .setOutputGroupingColumns(groupCols);

      return this;
    }

    /**
     * 设置当前节点输出Key
     *
     * @param keySchema
     *     Key定义
     * @return Builder对象
     */
    public Builder setOutputKeySchema(Column[] keySchema) {
      if (lastNode != null) {
        lastNode.setOutputKeySchema(keySchema);
      }

      return this;
    }

    /**
     * 设置当前节点输出Value Schema
     *
     * @param valueSchema
     *     Value定义
     * @return Builder对象
     */
    public Builder setOutputValueSchema(Column[] valueSchema) {
      if (lastNode != null) {
        lastNode.setOutputValueSchema(valueSchema);
      }

      return this;
    }

    /**
     * 设置当前节点输出排序列
     *
     * @param sortCols
     *     排序列
     * @return Builder对象
     */
    public Builder setOutputKeySortColumns(String[] sortCols) {
      if (lastNode != null) {
        lastNode.setOutputKeySortColumns(sortCols);
      }

      return this;
    }

    /**
     * 设置当前节点输出排序方式（升序或者降序）
     *
     * @param order
     *     排序方式
     * @return Builder对象
     */
    public Builder setOutputKeySortOrder(SortOrder[] order) {
      if (lastNode != null) {
        lastNode.setOutputKeySortOrder(order);
      }

      return this;
    }

    /**
     * 设置当前节点切分列
     *
     * @param partCols
     *     切分列
     * @return Builder对象
     */
    public Builder setPartitionColumns(String[] partCols) {
      if (lastNode != null) {
        lastNode.setPartitionColumns(partCols);
      }

      return this;
    }

    /**
     * 设置当前节点切分的Class定义
     *
     * @param theClass
     *     切分class定义
     * @return Builder对象
     */
    public Builder setPartitionerClass(Class<? extends Partitioner> theClass) {
      if (lastNode != null) {
        lastNode.setPartitionerClass(theClass);
      }

      return this;
    }
    
    /**
     * 设置当前节点任务数
     *
     * @param n
     *     任务数
     * @return Builder对象
     */
    public Builder setNumTasks(int n) {
      if (lastNode != null) {
        lastNode.setNumTasks(n);
      }

      return this;
    }
    
    /**
     * 设置当前节点的内存资源大小，单位：MB，默认值：2048.
     *
     * @param mem
     *     内存大小
     * @return Builder对象
     */
    public Builder setMemoryForTask(int mem) {
      if (lastNode != null) {
        lastNode.setMemoryForTask(mem);
      }

      return this;
    }
    
    /**
     * 设置当前节点JVM虚拟机的内存资源大小，单位：MB，默认值：1024.
     *
     * @param mem
     *     内存大小
     * @return Builder对象
     */
    public Builder setMemoryForJVM(int mem) {
      if (lastNode != null) {
        lastNode.setMemoryForJVM(mem);
      }

      return this;
    }

    /**
     * 设置当前节点输出的分组列
     *
     * @param partCols
     *     分组列
     * @return Builder对象
     */
    public Builder setOutputGroupingColumns(String[] cols) {
      if (lastNode != null) {
        lastNode.setOutputGroupingColumns(cols);
      }

      return this;
    }

      /**
       * 设置当前节点key比较器的Class定义
       *
       * @param theClass
       *     切分class定义
       * @return Builder对象
       */
      @SuppressWarnings("rawtypes")
      public Builder setOutputKeyComparatorClass(Class<? extends RecordComparator> theClass) {
        if (lastNode != null) {
          lastNode.setOutputKeyComparatorClass(theClass);
        }

        return this;
      }
      
      /**
       * 设置当前节点key分组比较器的Class定义
       *
       * @param theClass
       *     切分class定义
       * @return Builder对象
       */
      @SuppressWarnings("rawtypes")
      public Builder setOutputKeyGroupingComparatorClass(Class<? extends RecordComparator> theClass) {
        if (lastNode != null) {
          lastNode.setOutputKeyGroupingComparatorClass(theClass);
        }

        return this;
      }

    /**
     * 由Pipeline Builder产生Pipeline对象
     *
     * @return 一个新的Pipeline对象
     */
    public Pipeline createPipeline() {
      return new Pipeline(nodes);
    }
  }

  /**
   * 创建一个Pipeline Builder
   *
   * @return builder对象
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * 将Pipeline对象序列化到JobConf中：
   *
   * <pre>
   * odps.pipeline.list=map:com.example.Map1,reduce:com.example.Reduce1
   * odps.pipeline.0.output.key.schema=count:int
   * odps.pipeline.0.output.value.schema=word:string
   * ...
   * odps.pipeline.n.partitioner.class=com.example.Partitioner
   * </pre>
   *
   * @param job
   *     JobConf对象
   */
  public static void toJobConf(JobConf conf, Pipeline pipeline) {
    StringBuilder sb = new StringBuilder();
    List<TransformNode> nodes = pipeline.getNodes();
    for (int i = 0; i < nodes.size(); i++) {
      TransformNode node = nodes.get(i);
      sb.append(node.type);
      sb.append(":");
      sb.append(node.getTransformClass().getName());
      if (i != nodes.size() - 1) {
        sb.append(",");
      }

      if (node.getOutputKeySchema() != null) {
        conf.set(PIPELINE + i + OUTPUT_KEY_SCHEMA,
                 SchemaUtils.toString(node.getOutputKeySchema()));
      }

      if (node.getOutputValueSchema() != null) {
        conf.set(PIPELINE + i + OUTPUT_VALUE_SCHEMA,
                 SchemaUtils.toString(node.getOutputValueSchema()));
      }

      if (node.getOutputKeySortColumns() != null) {
        conf.set(PIPELINE + i + OUTPUT_KEY_SORT_COLUMNS,
                 StringUtils.join(node.getOutputKeySortColumns(), ","));
      }

      conf.set(PIPELINE + i + OUTPUT_KEY_SORT_ORDER,
          StringUtils.join(node.getOutputKeySortOrder(), ","));

      if (node.getPartitionColumns() != null) {
        conf.set(PIPELINE + i + PARTITION_COLUMNS,
                 StringUtils.join(node.getPartitionColumns(), ","));
      }

      if (node.getPartitionerClass() != null) {
        conf.set(PIPELINE + i + PARTITION_CLASS,
                 node.getPartitionerClass().getName());
      }

      if (node.getOutputGroupingColumns() != null) {
        conf.set(PIPELINE + i + OUTPUT_GROUP_COLUMNS,
                 StringUtils.join(node.getOutputGroupingColumns(), ","));
      }

      if (node.getOutputKeyComparatorClass() != null) {
        conf.set(PIPELINE + i + OUTPUT_KEY_COMPARATOR_CLASS, 
            node.getOutputKeyComparatorClass().getName());
      }

      if (node.getOutputKeyGroupingComparatorClass() != null) {
        conf.set(PIPELINE + i + OUTPUT_KEY_GROUPING_COMPARATOR_CLASS, 
            node.getOutputKeyGroupingComparatorClass().getName());
      }

      if (node.getNumTasks() >= 0) {
        if (i == 0) {
          conf.setInt("odps.stage.mapper.num", node.getNumTasks());
        } else {
          conf.setInt("odps.stage.reducer." + i + ".num", node.getNumTasks());
        }
      }

      if (node.getMemoryForTask() >= 0) {
        if (i == 0) {
          conf.setInt("odps.stage.mapper.mem", node.getMemoryForTask());
        } else {
          conf.setInt("odps.stage.reducer." + i + ".mem", node.getMemoryForTask());
        }
      }

      if (node.getMemoryForJVM() >= 0) {
        if (i == 0) {
          conf.setInt("odps.stage.mapper.jvm.mem", node.getMemoryForJVM());
        } else {
          conf.setInt("odps.stage.reducer." + i + ".jvm.mem", node.getMemoryForJVM());
        }
      }
    }

    conf.set(PIPELINE_LIST, sb.toString());
  }

  /**
   * 从JobConf中获得Pipeline对象。如果JobConf中没有相应的配置，返回null
   *
   * @param job
   *     JobConf
   * @return pipeline对象
   */
  public static Pipeline fromJobConf(JobConf conf) {
    String pipes = conf.get(PIPELINE_LIST);
    if (pipes == null) {
      return null;
    }

    Builder builder = builder();
    String[] pipelist = pipes.split(",");
    for (int i = 0; i < pipelist.length; i++) {
      String pipe = pipelist[i];
      String[] parts = pipe.split(":");

      // set class name
      try {
        Class<?> cls = conf.getClassByName(parts[1]);
        if (cls != null) {
          if (parts[0].equals("map")) {
            if (!Mapper.class.isAssignableFrom(cls)) {
              throw new RuntimeException(cls + " not Mapper");
            } else {
              builder.addMapper(cls.asSubclass(Mapper.class));
            }
          } else if (parts[0].equals("reduce")) {
            if (!Reducer.class.isAssignableFrom(cls)) {
              throw new RuntimeException(cls + " not Reducer");
            } else {
              builder.addReducer(cls.asSubclass(Reducer.class));
            }
          }
        } else {
          throw new RuntimeException("Class " + parts[1] + " not found");
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Class " + parts[1] + " not found");
      }

      // other properties
      String keySchema = conf.get(PIPELINE + i + OUTPUT_KEY_SCHEMA);
      if (keySchema != null) {
        builder.setOutputKeySchema(SchemaUtils.fromString(keySchema));
      }

      String valueSchema = conf.get(PIPELINE + i + OUTPUT_VALUE_SCHEMA);
      if (valueSchema != null) {
        builder.setOutputValueSchema(SchemaUtils.fromString(valueSchema));
      }

      String sortCols = conf.get(PIPELINE + i + OUTPUT_KEY_SORT_COLUMNS);
      if (sortCols != null) {
        builder.setOutputKeySortColumns(sortCols.split(","));
      }

      String joined = conf.get(PIPELINE + i + OUTPUT_KEY_SORT_ORDER);
      SortOrder[] order;
      if (joined != null && !joined.isEmpty()) {
        String[] orders = joined.split(",");
        order = new SortOrder[orders.length];
        for (int j = 0; j < order.length; j++) {
          order[j] = SortOrder.valueOf(orders[j]);
        }
        builder.setOutputKeySortOrder(order);
      }

      String partCols = conf.get(PIPELINE + i + PARTITION_COLUMNS);
      if (partCols != null && !partCols.isEmpty()) {
        builder.setPartitionColumns(partCols.split(","));
      }

      String partClass = conf.get(PIPELINE + i + PARTITION_CLASS);
      if (partClass != null && !partClass.isEmpty()) {
        try {
          Class<?> cls = conf.getClassByName(partClass);
          if (cls != null) {
            if (!Partitioner.class.isAssignableFrom(cls)) {
              throw new RuntimeException(cls + " not Partitioner");
            } else {
              builder.setPartitionerClass(cls.asSubclass(Partitioner.class));
            }
          } else {
            throw new RuntimeException("Class " + partClass + " not found");
          }
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Class " + partClass + " not found");
        }
      }

      String groupCols = conf.get(PIPELINE + i + OUTPUT_GROUP_COLUMNS);
      if (groupCols != null && !groupCols.isEmpty()) {
        builder.setOutputGroupingColumns(groupCols.split(","));
      }

      String keyCmpClass = conf.get(PIPELINE + i + OUTPUT_KEY_COMPARATOR_CLASS);
      if (keyCmpClass != null && !keyCmpClass.isEmpty()) {
        try {
          Class<?> cls = conf.getClassByName(keyCmpClass);
          if (cls != null) {
            if (!RecordComparator.class.isAssignableFrom(cls)) {
              throw new RuntimeException(cls + " not RecordComparator");
            } else {
              builder.setOutputKeyComparatorClass(cls.asSubclass(RecordComparator.class));
            }
          } else {
            throw new RuntimeException("Class " + keyCmpClass + " not found");
          }
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Class " + keyCmpClass + " not found");
        }
      }

      String keyGrpCmpClass = conf.get(PIPELINE + i + OUTPUT_KEY_GROUPING_COMPARATOR_CLASS);
      if (keyGrpCmpClass != null && !keyGrpCmpClass.isEmpty()) {
        try {
          Class<?> cls = conf.getClassByName(keyGrpCmpClass);
          if (cls != null) {
            if (!RecordComparator.class.isAssignableFrom(cls)) {
              throw new RuntimeException(cls + " not RecordComparator");
            } else {
              builder.setOutputKeyGroupingComparatorClass(cls.asSubclass(RecordComparator.class));
            }
          } else {
            throw new RuntimeException("Class " + keyGrpCmpClass + " not found");
          }
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Class " + keyGrpCmpClass + " not found");
        }
      }

      int numTasks = 1;
      if (i == 0) {
        numTasks = conf.getInt("odps.stage.mapper.num", 1);
      } else {
        numTasks = conf.getInt("odps.stage.reducer." + i + ".num", conf.getInt("odps.stage.reducer.num", 1));
      }
      builder.setNumTasks(numTasks);
    }
    return builder.createPipeline();
  }
}
