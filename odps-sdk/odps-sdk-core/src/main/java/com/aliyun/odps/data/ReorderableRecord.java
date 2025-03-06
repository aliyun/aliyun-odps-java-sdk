package com.aliyun.odps.data;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.NestedTypeInfo;
import com.aliyun.odps.type.TypeInfo;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ReorderableRecord extends ArrayRecord {

  @Override
  public void set(int idx, Object value) {
    TypeInfo typeInfo = getColumns()[idx].getTypeInfo();
    if (typeInfo instanceof NestedTypeInfo) {
      value = ReorderableStruct.reorderNestedType(value, null, typeInfo);
    }
    super.set(idx, value);
  }

  public ReorderableRecord(Column[] columns) {
    super(columns);
  }

  public ReorderableRecord(Column[] columns, boolean strictTypeValidation) {
    super(columns, strictTypeValidation);
  }

  public ReorderableRecord(Column[] columns, boolean strictTypeValidation, Long fieldMaxSize) {
    super(columns, strictTypeValidation, fieldMaxSize);
  }

  public ReorderableRecord(Column[] columns, Object[] values) {
    super(columns, values);
  }

  public ReorderableRecord(Column[] columns, Object[] values, boolean strictTypeValidation) {
    super(columns, values, strictTypeValidation);
  }

  public ReorderableRecord(TableSchema schema) {
    super(schema);
  }

  public ReorderableRecord(TableSchema schema, boolean strictTypeValidation) {
    super(schema, strictTypeValidation);
  }
}
