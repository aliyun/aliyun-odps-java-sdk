package com.aliyun.odps.data;

import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.ListIterator;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
import com.aliyun.odps.task.SQLTask;

@Deprecated
public class SessionQueryResult {
  private Iterator<Session.SubQueryResponse> rawResultIterator;
  private Session.SubQueryInfo subQueryInfo;
  private Integer status = Session.OBJECT_STATUS_RUNNING;

  public SessionQueryResult(Session.SubQueryInfo subQueryInfo, Iterator<Session.SubQueryResponse> result) {
    this.rawResultIterator = result;
    this.subQueryInfo = subQueryInfo;
  }

  /**
   * 获取结果记录迭代器
   *
   * @return 数据结果 record 迭代器
   */
  @Deprecated
  public Iterator<Record> getRecordIterator() {
    return new ListIterator<Record>() {
      @Override
      protected List<Record> list() {
        Session.SubQueryResponse response = null;
        try {
          if (rawResultIterator.hasNext()) {
            response = rawResultIterator.next();
            if (response.status == Session.OBJECT_STATUS_FAILED) {
              throw new OdpsException("Query failed:" + response.result);
            } else {
              return SQLTask.parseCsvRecord(response.result);
            }
          }

          return null;
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), e);
        }
      }
    };
  }

  /**
   * 获取结果数据迭代器
   *
   * @return 数据结果迭代器
   */
  public Iterator<Session.SubQueryResponse> getResultIterator() {
    return rawResultIterator;
  }

  /**
   * 获取数据结果
   *
   * @return 数据结果字符串
   */
  public String getResult() {
    StringBuilder stringBuilder = new StringBuilder();

    while(rawResultIterator.hasNext()) {
      Session.SubQueryResponse response = rawResultIterator.next();
      stringBuilder.append(response.result);
      status = response.status;
    }

    return stringBuilder.toString();
  }

  // Todo: implement this when session support instance tunnel
  // public ResultSet getResultSet(){}

  public Session.SubQueryInfo getSubQueryInfo() {
    return subQueryInfo;
  }

  public Integer getStatus() {
    return status;
  }
}
