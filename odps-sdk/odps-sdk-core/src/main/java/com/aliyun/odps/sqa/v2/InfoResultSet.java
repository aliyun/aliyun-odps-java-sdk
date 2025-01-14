package com.aliyun.odps.sqa.v2;


import java.util.Collections;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class InfoResultSet extends ResultSet {

  private final String info;

  private InfoResultSet(Record record, TableSchema schema, long recordCount) {
    super(new InMemoryRecordIterator(Collections.singletonList(record)), schema, recordCount);
    info = record.getString(0);
  }

  public static InfoResultSet of(String info) {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("Info", TypeInfoFactory.STRING));
    Record record = new ArrayRecord(schema);
    record.set(0, info);
    return new InfoResultSet(record, schema, 1);
  }

  public String getInfo() {
    return info;
  }
}
