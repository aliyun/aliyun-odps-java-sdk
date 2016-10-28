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

package com.aliyun.odps.mapred.unittest;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.utils.SchemaUtils;

/**
 * MapReduce Unit Test 上下文基类.
 * 
 * <p>
 * {@link MapUTContext} 和 {@link ReduceUTContext} 的基类，定义一些共同的方法。
 * 
 */
public abstract class UTContext {

  private int taskId = 0;

  protected RuntimeContext runtimeContext;
  private Map<String, String> outputSchemas = new HashMap<String, String>();

  private Set<String> resources = new HashSet<String>();
  private Map<String, byte[]> fileResources = new HashMap<String, byte[]>();
  private Map<String, File> archiveResources = new HashMap<String, File>();
  private Map<String, List<Record>> tableResources = new HashMap<String, List<Record>>();
  private Map<String, TableMeta> tableMetas = new HashMap<String, TableMeta>();

  private boolean isCleanUtDir = false; 

  /**
   * 返回设置的TaskId，不设默认为 0.
   * 
   * @return TaskId
   */
  public int getTaskId() {
    return taskId;
  }

  /**
   * 设置 TaskId，不设默认为 0.
   * 
   * <p>
   * 运行时可以从 TaskAttemptID#getTaskId() 取得。
   * 
   * @param taskId
   */
  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }



  /**
   * 给定本地资源文件，设置文件类型资源.
   * 
   * <p>
   * 如果在 {@link Mapper} 或 {@link Reducer} 里用到文件资源，可以通过本方法预先定义文件资源。
   * 
   * @param resourceName
   *          资源名称
   * @param file
   *          本地资源文件
   * @throws IOException
   */
  public void setFileResource(String resourceName, File file)
      throws IOException {
    checkResource(resourceName, file);
    setFileResource(resourceName, FileUtils.readFileToByteArray(file));
  }

  /**
   * 给定文件内容，设置文件类型资源.
   * 
   * <p>
   * 如果在 {@link Mapper} 或 {@link Reducer} 里用到文件资源，可以通过本方法预先定义文件资源。
   * 
   * @param resourceName
   *          资源名称
   * @param content
   *          文件内容
   * @throws IOException
   */
  public void setFileResource(String resourceName, String content)
      throws IOException {
    checkResource(resourceName);
    setFileResource(resourceName, (content == null ? null : content.getBytes()));
  }

  /**
   * 给定文件内容，设置文件类型资源.
   * 
   * <p>
   * 如果在 {@link Mapper} 或 {@link Reducer} 里用到文件资源，可以通过本方法预先定义文件资源。
   * 
   * @param resourceName
   *          资源名称
   * @param content
   *          文件内容
   * @throws IOException
   */
  public void setFileResource(String resourceName, byte[] content)
      throws IOException {
    checkResource(resourceName);
    if (content == null) {
      throw new IOException("content is null for resource: " + resourceName);
    }
    fileResources.put(resourceName, content);
    resources.add(resourceName);
  }

  /**
   * 定义压缩档案资源.
   * 
   * <p>
   * 如果在 {@link Mapper} 或 {@link Reducer} 里用到压缩档案资源，可以通过本方法预先定义。
   * 
   * @param resourceName
   *          资源名称
   * @param path
   *          本地压缩文件或本地压缩文件解压的文件目录
   * @throws IOException
   */
  public void setArchiveResource(String resourceName, File path)
      throws IOException {
    checkResource(resourceName, path);
    archiveResources.put(resourceName, path);
    resources.add(resourceName);
  }

  /**
   * 给定记录列表和表信息，设置表类型资源.
   * 
   * <p>
   * 如果在 {@link Mapper} 或 {@link Reducer} 里用到表类型资源，可以通过本方法预先定义。
   * 
   * <p>
   * 给定 schema 的格式为：[<proj>.<table_name>,]<col_name>:<col_type
   * >(,<col_name>:<col_type>)*<br />
   * 例如：
   * <ul>
   * <li>proj.tablename,a:string,b:bigint,c:double
   * <li>a:string,b:bigint,c:double
   * </ul>
   * 
   * @param resourceName
   *          资源名称
   * @param info
   *          表信息
   * @param schema
   *          表 schema
   * @param records
   *          表的记录集合
   * @throws IOException
   */
  public void setTableResource(String resourceName, TableInfo info,
      String schema, List<Record> records) throws IOException {
    checkResource(resourceName);

    if (info == null) {
      throw new IOException("table info is null for resource: " + resourceName);
    }
    if (schema == null) {
      throw new IOException("schema is null for resource: " + resourceName);
    }
    if (records == null) {
      throw new IOException("record list is null for resource: " + resourceName);
    }

    Column[] parts = new Column[info.getPartSpec().size()];
    int i = 0;
    for (Map.Entry<String, String> part : info.getPartSpec().entrySet()) {
      Column col = new Column(part.getKey(), OdpsType.STRING);
      parts[i++] = col;
    }
    TableMeta meta =
        new TableMeta(info.getProjectName(), info.getTableName(),
            com.aliyun.odps.mapred.utils.SchemaUtils.fromString(schema), parts);

    tableResources.put(resourceName, records);
    tableMetas.put(resourceName, meta);
    resources.add(resourceName);
  }

  /**
   * 给定本地表目录，设置表类型资源.
   * 
   * <p>
   * 一个本地表目录至少包含一个名为"__schema__"的 schema 文件，其他文件都为数据文件，格式为标准的csv格式。
   * 
   * @param resourceName
   *          资源名称
   * @param dir
   *          本地表目录
   * @throws IOException
   */
  public void setTableResource(String resourceName, File dir)
      throws IOException {
    checkResource(resourceName, dir);
    TableMeta meta = SchemaUtils.readSchema(dir);
    String schemaStr = SchemaUtils.toString(meta.getCols());
    
    TableInfo info = new TableInfo();
    info.setProjectName(meta.getProjName());
    info.setTableName(meta.getTableName());
    List<Record> records = MRUnitTest.readRecords(dir);
    setTableResource(resourceName, info, schemaStr, records);
  }

  /**
   * 设置默认输出的 schema.
   * 
   * <p>
   * schema 的格式为：<col_name>:<col_type >(,<col_name>:<col_type>)*<br />
   * 例如：a:string,b:bigint,c:double
   * 
   * <p>
   * 如果通过
   * {@link TableOutputFormat#addOutput(TableInfo, com.aliyun.odps.conf.Configuration)}
   * 设置了默认输出，则需要使用本方法设置默认输出的 schema。
   * 
   * @param schema
   *          默认输出的 schema
   * @throws IOException
   */
  public void setOutputSchema(String schema, Configuration conf) throws IOException {
    setOutputSchema("__default__", schema, conf);
  }

  /**
   * 给定输出的标签，设置 schema.
   * 
   * <p>
   * schema 的格式为：<col_name>:<col_type >(,<col_name>:<col_type>)*<br />
   * 例如：a:string,b:bigint,c:double
   * 
   * <p>
   * 如果通过
   * {@link TableOutputFormat#addOutput(TableInfo, String, com.aliyun.odps.conf.Configuration)}
   * 设置了给定标签的输出，则需要使用本方法设置其 schema。
   * 
   * @param label
   *          输出的标签
   * @param schema
   *          输出的 schema
   * @throws IOException
   */
  public void setOutputSchema(String label, String schema, Configuration conf) throws IOException {
    try {
      Column[] columns = com.aliyun.odps.mapred.utils.SchemaUtils.fromString(schema.trim());
      conf.set("odps.mapred.output.schema." + label, schema);
    } catch (Exception ex) {
      throw new IOException("bad schema format: " + schema);
    }
    outputSchemas.put(label, schema);
  }

  private void checkResource(String resourceName) throws IOException {
    if (StringUtils.isEmpty(resourceName)) {
      throw new IOException("invalid resource name: " + resourceName);
    }
    if (resources.contains(resourceName)) {
      throw new IOException("duplicate resource: " + resourceName);
    }
  }

  private void checkResource(String resourceName, File file) throws IOException {
    checkResource(resourceName);
    if (file == null) {
      throw new IOException("file or directory is null for resource: "
          + resourceName);
    }
    if (!file.exists()) {
      throw new IOException("file or directory not found for resource: "
          + resourceName + ", file: " + file);
    }
  }

  RuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  void setRuntimeContext(RuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
  }

  Map<String, String> getOutputSchemas() {
    return outputSchemas;
  }

  Map<String, byte[]> getFileResources() {
    return fileResources;
  }

  Map<String, File> getArchiveResources() {
    return archiveResources;
  }

  Map<String, List<Record>> getTableResources() {
    return tableResources;
  }

  Map<String, TableMeta> getTableMetas() {
    return tableMetas;
  }

  void clearResources() {
    resources.clear();
    fileResources.clear();
    archiveResources.clear();
    tableMetas.clear();
  }

  public boolean isCleanUtDir() {
    return isCleanUtDir;
  }

  public void setCleanUtDir(boolean isClean) {
    isCleanUtDir = isClean;
  }

}
