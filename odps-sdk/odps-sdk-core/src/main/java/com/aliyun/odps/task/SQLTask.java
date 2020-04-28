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

package com.aliyun.odps.task;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.*;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Project;
import com.aliyun.odps.Survey;
import com.aliyun.odps.Task;
import com.aliyun.odps.commons.util.EmptyIterator;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.utils.CSVRecordParser;
import com.aliyun.odps.utils.StringUtils;
import com.csvreader.CsvReader;
import com.google.gson.*;

/**
 * SQLTask的定义
 *
 * @author shenggong.wang@alibaba-inc.com
 */
@Root(name = "SQL", strict = false)
public class SQLTask extends Task {

  @Element(name = "Query", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  private String query;
  
  private static Map<String,String> defaultHints;

  private static final String AnonymousSQLTaskName = "AnonymousSQLTask";

  public String getQuery() {
    return query;
  }
  
  public static void setDefaultHints(Map<String,String> hints){
    SQLTask.defaultHints = hints;
  }
  
  public static void removeDefaultHints(){
    SQLTask.defaultHints = null;
  }

  /**
   * 设置SQL查询语句
   *
   * @param query
   *     需要执行的SQL查询
   */
  public void setQuery(String query) {
    this.query = query;
  }


  /**
   * 解析 CSV 格式的数据字符串，转换为 record 列表
   * @param csvResult 数据字符串
   * @return 记录列表
   *
   * @throws OdpsException
   */
  public static List<Record> parseCsvRecord(String csvResult) throws OdpsException {
    return CSVRecordParser.parse(csvResult).getRecords();
  }

  /**
   * Return 1W records at most. <br />
   *
   * Columns of each record in result are ALL OdpsType.STIRNG, ignore their real type in odps <br />
   *
   * Return value is valid only when SQL query is select, otherwise, result will be empty. <br />
   *
   * This API is used when SQLTask Instance is created with specific task name.
   *
   * <br />
   * Example:
   * <pre>
   * {
   *   String taskName = "test_select_sql_task";
   *   Instance i = SQLTask.run(odps, odps.getDefaultProject(),
   *                            "select * from test_select_sql_result;",
   *                            taskName, null, null, 3);
   *   instance.waitForSuccess();
   *   List<Record> records = SQLTask.getResult(i, taskName);
   * }
   * </pre>
   *
   * @param instance
   * @return
   * @throws OdpsException
   *
   * @see {@link #getResultByInstanceTunnel(Instance, String, Long)}
   */
  public static List<Record> getResult(Instance instance, String taskName) throws OdpsException {
    Map<String, String> results = instance.getTaskResults();
    String selectResult = results.get(taskName);

    if (selectResult != null) {
      return parseCsvRecord(selectResult);
    }
    return null;
  }

  /**
   * Return 1W records with completed schema info at most. No data size limit.<br />
   *
   * Return value is valid only when SQL query is select, otherwise, result will be empty. <br />
   *
   * This API is used when SQLTask Instance is created with specific task name.
   *
   * <br />
   * Example:
   * <pre>
   * {
   *   String taskName = "test_select_sql_task";
   *   Instance i = SQLTask.run(odps, odps.getDefaultProject(),
   *                            "select * from test_select_sql_result;",
   *                            taskName, null, null, 3);
   *   instance.waitForSuccess();
   *   List<Record> records = SQLTask.getResultByInstanceTunnel(i, taskName, 1000L);
   * }
   * </pre>
   *
   *
   * 使用 instance tunnel 的方式获取 task 结果.
   *
   * 本接口与 {@link #getResult(Instance, String)} 略有不同:
   *  1) 本接口返回的 record 带有完整 schema 信息;
   *  2) 本接口返回的 record 结果集有 1W 的条数限制，但不再有 10M 的大小限制
   *
   * 注意 : 返回结果类型为 {@link List}, 数据量较大时会带来较多内存开销,
   *       大数据量下载建议直接使用 {@see InstanceTunnel} 或 {@link #getResultSet(Instance, String)};
   *
   * @param instance
   *     instance 对象
   * @param taskName
   *     task 名称
   * @param limit
   *     获取结果的数量
   * @return record list
   * @throws OdpsException,
   *     IOException
   */
  @Survey
  public static List<Record> getResultByInstanceTunnel(Instance instance, String taskName,
                                                       Long limit)
      throws OdpsException, IOException {
    return getResultByInstanceTunnel(instance, taskName, limit, true);
  }

  private static List<Record> getResultByInstanceTunnel(Instance instance, String taskName,
                                                       Long limit, boolean limitEnabled)
      throws OdpsException, IOException {
    
    checkTaskName(instance, taskName);

    InstanceTunnel tunnel = new InstanceTunnel(instance.getOdps());
    InstanceTunnel.DownloadSession session =
        tunnel.createDownloadSession(instance.getProject(), instance.getId(), limitEnabled);
    
    long recordCount = session.getRecordCount();
    List<Record> records = new ArrayList<Record>();

    if (recordCount == 0) {
      return records;
    }

    if (limit != null && limit < recordCount) {
      recordCount = limit;
    }

    TunnelRecordReader reader = session.openRecordReader(0, recordCount);

    Record record;
    while ((record = reader.read()) != null) {
      records.add(record);
    }

    return records;
  }
  

  /**
   * 使用 instance tunnel 的方式获取 Anonymous task 的结果
   *
   * @param instance
   *     instance 对象
   * @param limit
   *     获取结果的数量
   * @return record list
   * @throws OdpsException,
   *     IOException
   *
   * @see #getResultByInstanceTunnel(Instance, String, Long)
   */
  @Survey
  public static List<Record> getResultByInstanceTunnel(Instance instance, Long limit)
      throws OdpsException, IOException {
    return getResultByInstanceTunnel(instance, AnonymousSQLTaskName, limit);
  }

  /**
   * 使用 instance tunnel 的方式获取 instance 结果
   *
   * 当数据量不大于 {@link #SQL_TASK_RESULT_LIMIT} 条时,成功;
   * 若数据量量大于 {@link #SQL_TASK_RESULT_LIMIT} 条, 返回 SQL_TASK_RESULT_LIMIT 条结果;
   *
   * 可使用 {@link #getResultByInstanceTunnel(Instance, String, Long)} 接口调整 limit 参数, 来获取更多数据;
   *
   * @param instance
   *     instance 对象
   * @param taskName
   *     task 名称
   * @return record list
   * @throws OdpsException,
   *     IOException
   */
  @Survey
  public static List<Record> getResultByInstanceTunnel(Instance instance, String taskName)
      throws OdpsException, IOException {
    return getResultByInstanceTunnel(instance, taskName, null);
  }

  /**
   * 使用 instance tunnel 的方式获取 Anonymous task 的结果
   *
   * @param instance
   *     instance 对象
   *
   * @return record list
   * @throws OdpsException,
   *     IOException
   *
   * @see #getResultByInstanceTunnel(Instance, String)
   */
  @Survey
  public static List<Record> getResultByInstanceTunnel(Instance instance)
      throws OdpsException, IOException {
    return getResultByInstanceTunnel(instance, AnonymousSQLTaskName);
  }

  /**
   * Return 1W records at most. <br />
   *
   * Columns of each record in result are ALL OdpsType.STIRNG, ignore their real type in odps <br
   * />
   * Return value is valid only when SQL query is select, otherwise, result will be empty. <br />
   * Default task name 'AnonymousSQLTask' will be used. <br />
   *
   * <br />
   * Example:
   * <pre>
   * {
   *    Instance i = SQLTask.run(odps, "select * from test_select_sql_result;");
   *    instance.waitForSuccess();
   *    List<Record> records = SQLTask.getResult(i);
   * }
   * </pre>
   *
   * @param instance
   * @return
   * @throws OdpsException
   */
  public static List<Record> getResult(Instance instance) throws OdpsException {
    return getResult(instance, AnonymousSQLTaskName);
  }
  
  /**
   * 通过instance获取记录迭代器，从而可以让用户通过迭代器逐条获取记录来避免一次性获取全量数据到本地时撑爆内存的问题
   *
   * 注：本接口使用 instance tunnel 实现，没有记录数限制，可获取instance对应query结果集的全量数据。
   * 但是只有instance的owner本人可以使用本接口，且当对应project打开protection时，
   * 需要提前在policy中为对应SQL中涉及的相应表和视图添加exception，否则无权下载
   * 
   * @param instance
   * @return
   * @throws OdpsException
   */
  public static ResultSet getResultSet(Instance instance) throws OdpsException, IOException {
    return getResultSet(instance, AnonymousSQLTaskName);
  }

  /**
   * 通过instance获取记录迭代器，从而可以让用户通过迭代器逐条获取记录来避免一次性获取全量数据到本地时撑爆内存的问题
   * 
   * 注：本接口使用 instance tunnel 实现，没有记录数限制，可获取instance对应query结果集的全量数据。
   * 但是只有instance的owner本人可以使用本接口，且当对应project打开protection时，
   * 需要提前在policy中为对应SQL中涉及的相应表和视图添加exception，否则无权下载
   * 
   * @param instance
   * @param taskName
   * @return
   * @throws OdpsException
   */
  public static ResultSet getResultSet(Instance instance, String taskName)
      throws OdpsException, IOException {
    return getResultSet(instance, taskName, null);
  }

  /**
   * 通过instance获取记录迭代器，从而可以让用户通过迭代器逐条获取记录来避免一次性获取全量数据到本地时撑爆内存的问题
   * 
   * 注：本接口使用 instance tunnel 实现，没有记录数限制，可获取instance对应query结果集的全量数据。
   * 但是只有instance的owner本人可以使用本接口，且当对应project打开protection时，
   * 需要提前在policy中为对应SQL中涉及的相应表和视图添加exception，否则无权下载
   * 
   * @param instance
   * @param limit
   * @return
   * @throws OdpsException
   */
  public static ResultSet getResultSet(Instance instance, Long limit) throws OdpsException, IOException {
    return getResultSet(instance, AnonymousSQLTaskName, limit);
  }

  /**
   * 通过instance获取记录迭代器，从而可以让用户通过迭代器逐条获取记录来避免一次性获取全量数据到本地时撑爆内存的问题
   * 
   * 注：本接口使用 instance tunnel 实现，没有记录数限制，可获取instance对应query结果集的全量数据。
   * 但是只有instance的owner本人可以使用本接口，且当对应project打开protection时，
   * 需要提前在policy中为对应SQL中涉及的相应表和视图添加exception，否则无权下载
   * 
   * @param instance
   * @param taskName
   * @param limit
   * @return
   * @throws OdpsException
   */
  public static ResultSet getResultSet(Instance instance, String taskName, Long limit)
      throws OdpsException {
    return getResultSet(instance, taskName, limit, false);
  }


  /**
   * 通过instance获取记录迭代器，从而可以让用户通过迭代器逐条获取记录来避免一次性获取全量数据到本地时撑爆内存的问题
   *
   * 注：
   * 1.只有instance的owner本人可以使用本接口
   * 2.当limitHint为true时，结果最多只能获得1条记录，超过将截断，但无需进行逐表的权限检查
   * 3.当limitHint为false时，没有记录数限制，可获取instance对应query结果集的全量数据。但前提是需要逐表（SQL中
   * 涉及的表与视图）对用户进行权限检查，所以当查询涉及表所在project打开protection时，需要提前在policy中为相应表
   * 和视图添加exception，否则无权下载
   *
   * @param instance
   * @param taskName
   * @param limit
   * @param limitHint
   * @return
   * @throws OdpsException
   */
  public static ResultSet getResultSet(Instance instance, String taskName, Long limit, boolean limitHint)
      throws OdpsException {
    return getResultSet(instance, taskName, limit, limitHint, null);
  }

  /**
   * 通过instance获取记录迭代器，从而可以让用户通过迭代器逐条获取记录来避免一次性获取全量数据到本地时撑爆内存的问题
   *
   * 注：本接口使用 instance tunnel 实现，没有记录数限制，可获取instance对应query结果集的全量数据。
   *
   * 1.只有instance的owner本人可以使用本接口
   * 2.当limitHint为true时，结果最多只能获得1条记录，超过将截断，但无需进行逐表的权限检查
   * 3.当limitHint为false时，没有记录数限制，可获取instance对应query结果集的全量数据。但前提是需要逐表（SQL中
   * 涉及的表与视图）对用户进行权限检查，所以当查询涉及表所在project打开protection时，需要提前在policy中为相应表
   * 和视图添加exception，否则无权下载
   * 
   * @param instance
   * @param taskName
   * @param limit
   * @param limitHint
   * @param tunnelEndpoint 指定 tunnel endpoint
   * @return
   * @throws OdpsException
   */
  public static ResultSet getResultSet(Instance instance, String taskName, Long limit,
                                       boolean limitHint, URI tunnelEndpoint)
      throws OdpsException {

    checkTaskName(instance, taskName);

    InstanceTunnel tunnel = new InstanceTunnel(instance.getOdps());
    if (tunnelEndpoint != null) {
      tunnel.setEndpoint(tunnelEndpoint.toString());
    }
    InstanceTunnel.DownloadSession session =
        tunnel.createDownloadSession(instance.getProject(), instance.getId(), limitHint);

    long recordCount = session.getRecordCount();

    if (recordCount == 0) {
      return new ResultSet(EmptyIterator.<Record>emptyIterator(), session.getSchema(), recordCount);
    }

    if (limit != null && limit < recordCount) {
      recordCount = limit;
    }

    return new ResultSet(new RecordSetIterator(session, recordCount), session.getSchema(), recordCount);
  }


  private static void checkTaskName(Instance instance, String taskName) throws OdpsException {

    if (StringUtils.isNullOrEmpty(taskName)) {
      throw new OdpsException("Invalid task name.");
    }

    boolean findTask = false;

    for (String n : instance.getTaskNames()) {
      if (taskName.equals(n)) {
        findTask = true;
        break;
      }
    }

    if (!findTask) {
      throw new OdpsException("Invalid task: " + taskName);
    }
  }

  @Override
  public String getCommandText() {
    return query;
  }


  /**
   * 获取运行 SQL 的编译警告信息，可能有多条.<br />
   *
   * @param instance
   *     instance 对象
   * @return 编译警告信息
   * @throws OdpsException
   */
  public static List<String> getSqlWarning(Instance instance) throws OdpsException {
    return getSqlWarning(instance, AnonymousSQLTaskName);
  }

  /**
   * 获取运行 SQL 的编译警告信息，可能有多条.<br />
   *
   * @param instance
   *     instance 对象
   * @param taskName
   *     task 名称
   * @return 编译警告信息
   * @throws OdpsException
   */
  public static List<String> getSqlWarning(Instance instance, String taskName) throws OdpsException {
    String warnings = instance.getTaskInfo(taskName, "warnings");

    try {
      List<String> warningList = new LinkedList<String>();
      JsonObject jsonObject = new JsonParser().parse(warnings).getAsJsonObject();
      if (!jsonObject.has("warnings")) {
        return null;
      }
      JsonArray array = jsonObject.get("warnings").getAsJsonArray();
      for (JsonElement element : array) {
        warningList.add(element.getAsString());
      }
      return warningList;
    } catch (JsonParseException e) {
      return null;
    }
  }

  /**
   * 运行 SQL.<br />
   *
   * 特别注意，在执行数据读取操作时：<br />
   * 正常情况下的 task 执行后，task 的状态为 SUCCESS，并正常返回数据结果。<br />
   * 但是，当读取数据量超过 10MB，task 的状态将是 FAILED，返回的数据结果为 error message。<br />
   * 因此,大量数据的获取建议使用 {@link com.aliyun.odps.tunnel.TableTunnel} 进行操作。<br />
   *
   * <br />
   * 示例代码：
   * <pre>
   * {
   *    String sql = "select ....;";
   *
   *    Instance instance = SQLTask.run(odps, sql);
   *
   *    instance.waitForSuccess();
   *
   *    Map<String, String> results = instance.getTaskResults();
   *    Map<String, TaskStatus> taskStatus = instance.getTaskStatus();
   *
   *    for(Entry<String, TaskStatus> status : taskStatus.entrySet()) {
   *        if (TaskStatus.Status.SUCCESS == status.getValue().getStatus()) {
   *           String result = results.get(status.getKey());
   *          System.out.println(result);
   *        }
   *    }
   * }
   * </pre>
   *
   * @param {@link
   *     Odps}
   * @param sql
   *     需要执行的SQL查询
   * @return 运行实例 {@link Instance}
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String sql) throws OdpsException {
    String project = odps.getDefaultProject();
    if (project == null) {
      throw new OdpsException("default project required.");
    }
    return run(odps, project, sql, "AnonymousSQLTask", null, null, "sql");
  }

  /**
   * 运行SQL
   *
   * @param odps
   *     {@link Odps}对象
   * @param project
   *     任务运行时所属的{@link Project}名称
   * @param sql
   *     需要运行的SQL查询
   * @param hints
   *     能够影响SQL执行的Set信息，例如：odps.mapred.map.split.size等
   * @param aliases
   *     Alias信息。详情请参考用户手册中alias命令的相关介绍
   * @return 作业运行实例 {@link Instance}
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String project, String sql,
                             Map<String, String> hints, Map<String, String> aliases)
      throws OdpsException {
    return run(odps, project, sql, AnonymousSQLTaskName, hints, aliases, "sql");
  }

  /*Un-document*/
  public static Instance run(Odps odps, String project, String sql,
                             String taskName, Map<String, String> hints,
                             Map<String, String> aliases) throws OdpsException {
    return run(odps, project, sql, taskName, hints, aliases, "sql");
  }

  /**
   * 运行SQL
   *
   * @param odps
   *     {@link Odps}对象
   * @param project
   *     任务运行时所属的{@link Project}名称
   * @param sql
   *     需要运行的SQL查询
   * @param taskName
   *     任务名称
   * @param hints
   *     能够影响SQL执行的Set信息，例如：odps.mapred.map.split.size等
   * @param aliases
   *     Alias信息。详情请参考用户手册中alias命令的相关介绍
   * @param priority
   *     作业优先级 (注：公共云环境此参数无效)
   * @return 作业运行实例 {@link Instance}
   * @throws OdpsException
   */
  public static Instance run(Odps odps, String project, String sql,
                             String taskName, Map<String, String> hints,
                             Map<String, String> aliases, int priority) throws OdpsException {
    return run(odps, project, sql, taskName, hints, aliases, priority, "sql");
  }

  private static Instance run(Odps odps, String project, String sql, String taskName,
                              Map<String, String> hints, Map<String, String> aliases,
                              Integer priority,
                              String type) throws OdpsException {
    SQLTask task = new SQLTask();
    task.setQuery(sql);
    task.setName(taskName);
    task.setProperty("type", type);
    
    if (hints == null) {
      hints = defaultHints;
    }

    if (hints != null) {
      try {
        String json = new GsonBuilder().disableHtmlEscaping().create().toJson(hints);
        task.setProperty("settings", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }

    if (aliases != null) {
      try {
        String json = new GsonBuilder().disableHtmlEscaping().create().toJson(aliases);
        task.setProperty("aliases", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }

    }

    if (priority != null) {
      return odps.instances().create(project, task, priority);
    } else {
      return odps.instances().create(project, task);
    }
  }

  static Instance run(Odps odps, String project, String sql,
                      String taskName, Map<String, String> hints, Map<String, String> aliases,
                      String type) throws OdpsException {
    return run(odps, project, sql, taskName, hints, aliases, null, type);
  }
}


class RecordSetIterator implements Iterator<Record> {

  private static final Long FETCH_SIZE = 1000l;

  private InstanceTunnel.DownloadSession session;
  private long recordCount;
  private long cursor = 0;
  private long fetchSize = 0;
  private int idx = 0;
  private List<Record> buffer;

  public RecordSetIterator(InstanceTunnel.DownloadSession session, long recordCount) {
    this.session = session;
    this.recordCount = recordCount;
  }

  @Override
  public boolean hasNext() {
    return cursor < recordCount;
  }

  @Override
  public Record next() {
    if (buffer == null || idx == buffer.size()) {
      fillBuffer();
    }
    cursor++;
    return buffer.get(idx++);
  }
  
  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  private void fillBuffer() {
    idx = 0;
    TunnelRecordReader reader = openNewReader();
    buffer = new ArrayList<Record>();
    Record r = null;
    try {
      while ((r = reader.read()) != null) {
        buffer.add(r);
      }
    } catch (IOException e) {
      throw new RuntimeException("Read from reader failed:", e);
    }
  }

  private TunnelRecordReader openNewReader() {
    fetchSize = recordCount - cursor <= FETCH_SIZE ? recordCount - cursor : FETCH_SIZE;
    try {
      return session.openRecordReader(cursor, fetchSize);
    } catch (TunnelException e) {
      throw new RuntimeException("Open reader failed: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new RuntimeException("Open reader failed: " + e.getMessage(), e);
    }
  }

}
