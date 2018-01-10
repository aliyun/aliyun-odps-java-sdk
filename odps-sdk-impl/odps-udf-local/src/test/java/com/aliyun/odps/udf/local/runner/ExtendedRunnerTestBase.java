package com.aliyun.odps.udf.local.runner;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.util.UnstructuredUtils;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExtendedRunnerTestBase {
  protected static Odps odps;
  @BeforeClass
  public static void setupBeforeClass() {
    Account account = new AliyunAccount("accessId", "accessKey");
    odps = new Odps(account);
    odps.setEndpoint("endpoint");
    odps.setDefaultProject("project_name");
  }

  protected Column[] parseSchemaString(String str){
    return UnstructuredUtils.parseSchemaString(str);
  }

  protected Column[] selectPartialColumns(Column[] fulLSchema, int[] neededIndexes) {
    List<Column> columns = new ArrayList<Column>(neededIndexes.length);
    for(int i = 0; i < neededIndexes.length; i++){
      if (neededIndexes[i] < 0 || neededIndexes[i] >= fulLSchema.length) {
        throw new UnsupportedOperationException("invalid index:" + neededIndexes[i]);
      }
      columns.add(fulLSchema[neededIndexes[i]]);
    }
    return columns.toArray(new Column[columns.size()]);
  }

  protected boolean recordsEqual(Record left, Record right){
    return UnstructuredUtils.recordsEqual(left, right);
  }

  /**
   * Read next record from internal table with minimum parameter setting.
   * This implies project name is set to the default "project_name", and a non-partitioned internal table is assumed.
   * @param tableName
   * @param tableSchema
   * @return next record from the table, or null when there is no more record to read
   */
  protected Record readFromInternalTable(String tableName, Column[] tableSchema)
      throws IOException, LocalRunException {
    return UnstructuredUtils.readFromInternalTable("project_name", tableName, tableSchema, null);
  }

}
