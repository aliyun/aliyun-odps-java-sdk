package com.aliyun.odps.udf.local.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.type.TypeInfoParser;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.datasource.TableInputSource;
import org.junit.Assert;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class UnstructuredUtils {

  private static Map<String, TableInputSource> internalTables = new HashMap<String, TableInputSource>();

  public static Column[] parseSchemaString(String str){
    if ( str == null || str.isEmpty()) {
      throw new IllegalArgumentException("Empty or null schema string.");
    }
    String[] parts = str.split(";");
    Column[] fullExternalTableSchema = new Column[parts.length];
    for(int i = 0; i < fullExternalTableSchema.length; i++){
      String[] namedValue = parts[i].split(":");
      if (namedValue.length != 2) {
        throw new IllegalArgumentException("Invalid schema: " + parts[i]);
      }
      fullExternalTableSchema[i] = new Column(namedValue[0], TypeInfoParser.getTypeInfoFromTypeString(namedValue[1]));
    }
    return fullExternalTableSchema;
  }

  // this will fail instead of returning false when there is mismatch
  // it only returns true when all assertions pass
  public static boolean recordsEqual(Record left, Record right){
    Column[] leftColumns = left.getColumns();
    Column[] rightColumns = right.getColumns();
    Assert.assertEquals(leftColumns.length, rightColumns.length);

    for (int i = 0; i < leftColumns.length; i++){
      Assert.assertEquals(leftColumns[i] , rightColumns[i]);
    }
    Object[] leftData = left.toArray();
    Object[] rightData = right.toArray();
    Assert.assertEquals(leftData.length, rightData.length);
    for (int i = 0; i < leftData.length; i++) {
      if (leftData[i] instanceof Double) {
        Assert.assertEquals((Double)(leftData[i]), (Double)(rightData[i]), 0.1);
      }
      else {
        Assert.assertEquals(leftData[i], rightData[i]);
      }
    }
    return true;
  }

  public static String generateOutputName() {
    return "output_" + LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_1).format(new Date());
  }

  /**
   * internal implementation for reading table
   */
  public static Record readFromInternalTable(String projectName, String tableName, Column[] tableSchema, String[] partitions)
      throws IOException, LocalRunException {
    String fullTableName = projectName + "." + tableName;
    TableInputSource tableInputSource = null;
    if (!internalTables.containsKey(fullTableName)){
      String[] columns = new String[tableSchema.length];
      for (int i = 0; i < columns.length; i++){
        columns[i] = tableSchema[i].getName();
      }
      tableInputSource = new TableInputSource(projectName, tableName, partitions, columns);
      internalTables.put(fullTableName, tableInputSource);
    } else {
      tableInputSource = internalTables.get(fullTableName);
    }
    Object[] data = tableInputSource.getNextRow();
    return data == null ? null : new ArrayRecord(tableSchema, data);
  }

}
