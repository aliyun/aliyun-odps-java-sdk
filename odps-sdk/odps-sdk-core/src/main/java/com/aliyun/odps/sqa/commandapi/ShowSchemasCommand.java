package com.aliyun.odps.sqa.commandapi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class ShowSchemasCommand extends SQLTaskCommand {

  private static final Map<String, TypeInfo> showSchemasMap = new LinkedHashMap<>();

  static {
    showSchemasMap.put("Info", TypeInfoFactory.STRING);
  }

  @Override
  public List<String> getResultHeaders() {
    return new ArrayList<>(showSchemasMap.keySet());
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return new ArrayList<>(showSchemasMap.values());
  }

  @Override
  public RecordIter run(Odps odps, CommandInfo commandInfo) throws OdpsException {
    String taskNamePrefix = "show_schemas_";
    return sqlTaskRun(odps, commandInfo, taskNamePrefix);
  }
}
