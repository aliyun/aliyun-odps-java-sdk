package com.aliyun.odps.data;

import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.ListIterator;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
import com.aliyun.odps.task.SQLTask;

public class SessionQueryResult {
  private Iterator<Session.SubQueryResponse> rawResultIterator;

  public SessionQueryResult(Iterator<Session.SubQueryResponse> result) {
    this.rawResultIterator = result;
  }

  /**
   * 获取结果记录迭代器
   *
   * @return 数据结果 record 迭代器
   */
  public Iterator<Record> getRecordIterator() {
    return new ListIterator<Record>() {
      @Override
      protected List<Record> list() {
        try {
          if (rawResultIterator.hasNext()) {
            return SQLTask.parseCsvRecord(rawResultIterator.next().result);
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
      stringBuilder.append(rawResultIterator.next().result);
    }

    return stringBuilder.toString();
  }

  // Todo: implement this when session support instance tunnel
  // public ResultSet getResultSet(){}

}
