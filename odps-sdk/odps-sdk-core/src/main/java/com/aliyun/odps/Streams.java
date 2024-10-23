package com.aliyun.odps;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.table.StreamIdentifier;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.NameSpaceSchemaUtils;
import com.aliyun.odps.utils.StringUtils;

/**
 * Stream 对象是ODPS中用于管理增量数据的对象，用户可以通过Stream对象对增量数据进行管理
 * Streams 对Stream对象进行管理
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Streams implements Iterable<Stream> {

  private final RestClient client;
  private final Odps odps;

  enum ReadMode {
    /**
     * Append模式，只会输出数据的最后状态
     * 适用于不需要delete/update状态计算逻辑的纯Append的增量链路场景
     */
    APPEND,
    /**
     * CDC模式
     * 额外输出两个系统列: __meta_op_type (包含INSERT | DELETE)和__meta_is_update (包含true | false);
     * 因此可以组合成四种情况: INSERT+FALSE代表新纪录，INSERT+TRUE代表Update后的值，DELETE+TRUE代表Update前的值，DELETE+FALSE代表删除。
     */
    CDC,
  }


  Streams(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  public Stream get(String streamName) {
    return get(getDefaultProjectName(), streamName);
  }

  public Stream get(StreamIdentifier identifier) {
    return get(identifier.getProject(), identifier.getStreamName());
  }


  public Stream get(String projectName, String streamName) {
    Stream.StreamModel streamModel = new Stream.StreamModel();
    streamModel.name = streamName;
    return new Stream(streamModel, projectName, odps);
  }

  /**
   * 判断指定表是否存在
   *
   * @param streamName 表名
   * @return 存在返回true, 否则返回false
   */
  public boolean exists(String streamName) throws OdpsException {
    return exists(getDefaultProjectName(), streamName);
  }

  public boolean exists(StreamIdentifier identifier) throws OdpsException {
    return exists(identifier.getProject(), identifier.getStreamName());
  }

  /**
   * 判断指定表是否存在
   *
   * @param projectName 所在{@link Project}名称
   * @param streamName  表名
   * @return 存在返回true, 否则返回false
   */
  public boolean exists(String projectName, String streamName) throws OdpsException {
    if (StringUtils.isNullOrEmpty(streamName)) {
      return false;
    }
    try {
      Stream t = get(projectName, streamName);
      t.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /**
   * 创建一个stream
   *
   * @param identifier stream信息{@link StreamIdentifier}
   * @param refTable   stream关联的ACID源表{@link TableIdentifier}
   */
  public void create(StreamIdentifier identifier, TableIdentifier refTable) throws OdpsException {
    create(identifier, refTable, false, null, null, ReadMode.APPEND, null);
  }

  /**
   * @param identifier  stream信息{@link StreamIdentifier}
   * @param refTable    stream关联的ACID源表{@link TableIdentifier}
   * @param ifNotExists true表示如果表已经存在，则不抛出异常，否则抛出异常
   * @param timestamp   内部LSN序号, 代表Stream对象创建时VersionOffset初始化数据版本为t，查询范围为(t, 最新增量数据版本]
   * @param version     MC标准的TIMESTAMP/DATETIME/DATE类型格式, 代表Stream对象创建时VersionOffset初始化数据版本为v，查询范围为(v, 最新增量数据版本]
   * @param readMode    CDC模式或Append模式，目前仅支持Append模式
   * @param comment     注释
   * @throws OdpsException if sql task run failed
   */
  public void create(StreamIdentifier identifier, TableIdentifier refTable, boolean ifNotExists,
                     String timestamp, Long version, ReadMode readMode, String comment)
      throws OdpsException {
    if (StringUtils.isBlank(timestamp) && version != null) {
      throw new InvalidParameterException(
          "Both timestamp and version cannot be specified at the same time");
    }
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE STREAM ");
    if (ifNotExists) {
      sql.append("IF NOT EXISTS ");
    }
    sql.append(identifier).append(" ON TABLE ");
    sql.append(refTable);
    if (StringUtils.isNotBlank(timestamp)) {
      sql.append(" TIMESTAMP AS OF ").append(timestamp);
    }
    if (version != null) {
      sql.append(" VERSION AS OF ").append(version);
    }
    sql.append(" STRMPROPERTIES(\"read_mode\"=\"").append(readMode.name().toLowerCase())
        .append("\")");
    if (StringUtils.isNotBlank(comment)) {
      sql.append(" COMMENT ").append(comment);
    }
    sql.append(";");
    Map<String, String> hints = new HashMap<>();
    NameSpaceSchemaUtils.setSchemaFlagInHints(hints, refTable.getSchema());
    Instance
        createStreamTask =
        SQLTask.run(odps, identifier.getProject(), sql.toString(), "SQLCreateStreamTask", hints,
                    null);
    createStreamTask.waitForSuccess();
  }

  public void delete(String streamName) throws OdpsException {
    delete(getDefaultProjectName(), streamName, false);
  }

  public void delete(String streamName, boolean ifExists) throws OdpsException {
    delete(getDefaultProjectName(), streamName, ifExists);
  }

  public void delete(String projectName, String streamName, boolean ifExists) throws OdpsException {
    StringBuilder sql = new StringBuilder("DROP STREAM ");
    if (ifExists) {
      sql.append("IF EXISTS ");
    }
    sql.append(streamName);
    sql.append(";");
    Instance
        dropStreamTask =
        SQLTask.run(odps, projectName, sql.toString(), "SQLDropStreamTask", null, null);
    dropStreamTask.waitForSuccess();
  }

  public Builder builder(StreamIdentifier identifier, TableIdentifier refTable) {
    return new Builder(odps, identifier, refTable);
  }

  class Builder {

    Odps odps;
    StreamIdentifier identifier;
    TableIdentifier refTable;
    boolean ifNotExists;
    Long version;
    String timestamp;
    ReadMode readMode;
    String comment;

    Builder(Odps odps, StreamIdentifier identifier, TableIdentifier refTable) {
      this.odps = odps;
      this.identifier = identifier;
      this.refTable = refTable;
    }

    public Builder withTimestamp(String timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder ifNotExists() {
      this.ifNotExists = true;
      return this;
    }

    public Builder withVersion(Long version) {
      this.version = version;
      return this;
    }

    public Builder withReadMode(ReadMode readMode) {
      this.readMode = readMode;
      return this;
    }

    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    public Stream build() throws OdpsException {
      create(identifier, refTable, ifNotExists, timestamp, version, readMode, comment);
      return odps.streams().get(identifier);
    }
  }

  /**
   * list streams
   */
  @Root(name = "StreamObjects", strict = false)
  private static class ListStreamsResponse {

    @ElementList(entry = "StreamObject", inline = true, required = false)
    private List<StreamObject> streams = new ArrayList<>();

    private static class StreamObject {

      @Element(name = "Name", required = false)
      @Convert(SimpleXmlUtils.EmptyStringConverter.class)
      private String name;

      StreamObject() {
      }

      StreamObject(String name) {
        this.name = name;
      }
    }

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;
  }

  @Override
  public Iterator<Stream> iterator() {
    return iterator(getDefaultProjectName());
  }

  public Iterator<Stream> iterator(String projectName) {
    return new StreamListIterator(projectName);
  }

  private class StreamListIterator extends ListIterator<Stream> {

    private final Map<String, String> params;
    private final String projectName;
    private static final String MARKER = "marker";
    private static final String MAX_ITEMS = "maxitems";

    StreamListIterator(String projectName) {
      this.projectName = projectName;
      params = new HashMap<>();
    }

    @Override
    protected List<Stream> list() {
      if (params.containsKey(MARKER) && params.get(MARKER).length() == 0) {
        return null;
      }
      List<Stream> streamList = new ArrayList<>();
      String resource = ResourceBuilder.buildStreamObjectResource(projectName);
      try {
        ListStreamsResponse
            resp =
            client.request(ListStreamsResponse.class, resource, "GET", params);
        for (ListStreamsResponse.StreamObject streamName : resp.streams) {
          streamList.add(get(projectName, streamName.name));
        }
        params.put(MARKER, resp.marker);
        return streamList;
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    @Override
    public List<Stream> list(String marker, long maxItems) {
      if (marker != null) {
        params.put(MARKER, marker);
      }
      if (maxItems >= 0) {
        params.put(MAX_ITEMS, String.valueOf(maxItems));
      }
      return list();
    }

    @Override
    public String getMarker() {
      // marker = empty string => end of loop
      return params.getOrDefault(MARKER, "");
    }
  }

  private String getDefaultProjectName() {
    String project = client.getDefaultProject();
    if (project == null || project.length() == 0) {
      throw new RuntimeException("No default project specified.");
    }
    return project;
  }
}
