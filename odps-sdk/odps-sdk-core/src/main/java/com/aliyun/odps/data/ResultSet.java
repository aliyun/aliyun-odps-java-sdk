/**
 * 
 */
package com.aliyun.odps.data;

import java.util.Iterator;

import com.aliyun.odps.TableSchema;

/**
 * ResultSet for SQLTask
 * 
 * @author emerson
 *
 */
public class ResultSet implements Iterable<Record>, Iterator<Record> {

  private Iterator<Record> recordIterator;
  private long recordCount;
  private TableSchema schema;

  public ResultSet(Iterator<Record> recordIterator, TableSchema schema, long recordCount) {
    this.recordIterator = recordIterator;
    this.recordCount = recordCount;
    this.schema = schema;
  }

  @Override
  public Iterator<Record> iterator() {
    return recordIterator;
  }

  public TableSchema getTableSchema() {
    return schema;
  }

  @Override
  public boolean hasNext() {
    return recordIterator.hasNext();
  }

  @Override
  public Record next() {
    return recordIterator.next();
  }
  
  @Override
  public void remove() {
    recordIterator.remove();
  }

  public long getRecordCount() {
    return recordCount;
  }
}
