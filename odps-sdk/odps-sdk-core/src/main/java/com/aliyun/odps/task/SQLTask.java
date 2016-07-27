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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.alibaba.fastjson.JSON;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Project;
import com.aliyun.odps.Task;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.utils.StringUtils;
import com.csvreader.CsvReader;

/**
 * SQLTask的定义
 *
 * @author shenggong.wang@alibaba-inc.com
 */
@XmlRootElement(name = "SQL")
public class SQLTask extends Task {

  private String query;

  private static final Long SQL_TASK_RESULT_LIMIT = 10000L;

  private static final String AnonymousSQLTaskName = "AnonymousSQLTask";

  public String getQuery() {
    return query;
  }

  /**
   * 设置SQL查询语句
   *
   * @param query
   *     需要执行的SQL查询
   */
  @XmlElement(name = "Query")
  public void setQuery(String query) {
    this.query = query;
  }

  /**
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
  @Deprecated
  public static List<Record> getResult(Instance instance, String taskName) throws OdpsException {
    Map<String, String> results = instance.getTaskResults();
    String selectResult = results.get(taskName);

    if (selectResult != null) {
      CsvReader reader = new CsvReader(new StringReader(selectResult));
      List<Record> records = new ArrayList<Record>();
      int lineCount = 0;
      String[] newline;
      Column[] columns = new Column[]{};

      try {
        while (reader.readRecord()) {
          newline = reader.getValues();
          // the first line is column names
          if (lineCount == 0) {
            columns = new Column[newline.length];
            for (int i = 0; i < newline.length; i++) {
              columns[i] = new Column(newline[i], OdpsType.STRING);
            }
          } else {
            Record record = new ArrayRecord(columns);
            for (int i = 0; i < newline.length; i++) {
              record.set(i, newline[i]);
            }
            records.add(record);
          }
          lineCount++;
        }
      } catch (IOException e) {
        throw new OdpsException("Error when parse sql results.");
      }
      return records;
    }
    return null;
  }

  /**
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
   *   List<Record> records = SQLTask.getResultByInstanceTunnel(i, taskName, 1000L);
   * }
   * </pre>
   *
   *
   * 使用 instance tunnel 的方式获取 task 结果.
   *
   * 本接口与 {@link #getResult(Instance, String)} 略有不同:
   *  1) 本接口返回的 record 带有完整 schema 信息;
   *  2) 本接口返回的 record 结果集不再有 1W 的条数限制.
   *
   * 注意 : 返回结果类型为 {@link List}, 数据量较大时会带来较多内存开销,
   *       大数据量下载建议直接使用 {@see InstanceTunnel};
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
  public static List<Record> getResultByInstanceTunnel(Instance instance, String taskName,
                                                       Long limit)
      throws OdpsException, IOException {
    if (StringUtils.isNullOrEmpty(taskName)) {
      throw new OdpsException("Invalid task name.");
    }

    Long queryLimit = null;
    boolean findTask = false;

    for (Task t : instance.getTasks()) {
      if (taskName.equals(t.getName())) {
        SQLTask task = (SQLTask) t;
        String query = task.getQuery().trim();

        if (!StringUtils.isNullOrEmpty(query)) {
          queryLimit = getLimitCount(query);
        }

        findTask = true;
        break;
      }
    }

    if (!findTask) {
      throw new OdpsException("Invalid task: " + taskName);
    }

    if (limit == null) {
      limit = SQL_TASK_RESULT_LIMIT;
    }

    // if query has a query limit, use the less one compared with limit;
    // if not, use limit.
    if (queryLimit != null) {
      limit = queryLimit < limit ? queryLimit : limit;
    }

    InstanceTunnel tunnel = new InstanceTunnel(instance.getOdps());
    InstanceTunnel.DownloadSession session = tunnel.createDownloadSession(instance.getProject(),
                                                                          instance.getId());
    long recordCount = session.getRecordCount();
    List<Record> records = new ArrayList<Record>();

    if (recordCount == 0) {
      return records;
    }

    if (limit < recordCount) {
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
  public static List<Record> getResultByInstanceTunnel(Instance instance)
      throws OdpsException, IOException {
    return getResultByInstanceTunnel(instance, AnonymousSQLTaskName);
  }

  private static Pattern PATTERN =
      Pattern.compile(".*LIMIT\\s+(\\d+)\\s*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static Long getLimitCount(String query) {
    if (query.endsWith(";")) {
      query = query.substring(0, query.length() - 1);
      query = query.trim();
    }

    Matcher m = PATTERN.matcher(query);

    if (m.matches()) {
      return Long.parseLong(m.group(1));
    }

    return null;
  }

  /**
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

  @Override
  public String getCommandText() {
    return query;
  }

  /**
   * 运行 SQL.<br />
   * 执行读取数据时，最多返回 1W 条记录，若超过，数据将被截断。<br />
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
   * @param alias
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

    if (hints != null) {
      try {
        String json = JSON.toJSONString(hints);
        task.setProperty("settings", json);
      } catch (Exception e) {
        throw new OdpsException(e.getMessage(), e);
      }

    }

    if (aliases != null) {
      try {
        String json = JSON.toJSONString(aliases);
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
