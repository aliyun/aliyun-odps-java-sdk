package com.aliyun.odps.sqa.commandapi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class ShowCreateTableCommand extends SQLTaskCommand {

  private static final Map<String, TypeInfo> showCreateTableMap = new LinkedHashMap<>();

  static {
    showCreateTableMap.put("Info", TypeInfoFactory.STRING);
  }

  @Override
  public List<String> getResultHeaders() {
    return new ArrayList<>(showCreateTableMap.keySet());
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return new ArrayList<>(showCreateTableMap.values());
  }

  @Override
  public RecordIter run(Odps odps, CommandInfo commandInfo) throws OdpsException {
    String taskNamePrefix = "show_create_table_";
    return sqlTaskRun(odps, commandInfo, taskNamePrefix);
  }
}
