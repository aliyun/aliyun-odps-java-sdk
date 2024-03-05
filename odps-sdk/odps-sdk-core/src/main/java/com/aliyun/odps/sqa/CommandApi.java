package com.aliyun.odps.sqa;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.sqa.commandapi.Command;
import com.aliyun.odps.sqa.commandapi.CommandInfo;
import com.aliyun.odps.sqa.commandapi.RecordIter;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;


class CommandApi {

  Odps odps;

  public CommandApi(Odps odps) {
    this.odps = odps;
  }

  CommandInfo commandInfo;

  boolean parseSuccess;

  public void init() {
    commandInfo = null;
    parseSuccess = false;
  }

  public void run(String commandText) throws OdpsException {
    run(commandText, new HashMap<>(), false);
  }

  public void run(String commandText, boolean odpsNamespaceSchema) throws OdpsException {
    run(commandText, new HashMap<>(), odpsNamespaceSchema);
  }

  public void run(String commandText, Map<String, String> settings, boolean odpsNamespaceSchema)
      throws OdpsException {
    commandInfo = new CommandInfo(commandText, settings);

    Command command = CommandUtil.parseCommand(commandText);
    if (command != null) {
      commandInfo.setCommand(command);
      commandInfo.setOdpsNamespaceSchema(odpsNamespaceSchema);
      if (!command.isSync()) {
        command.run(odps, commandInfo);
      }
      parseSuccess = true;
    } else {
      parseSuccess = false;
    }
  }

  public boolean isParseSuccess() {
    return parseSuccess;
  }

  public boolean isOdpsCommand(String commandText) {
    return CommandUtil.parseCommand(commandText) != null;
  }

  public boolean hasResultSet() {
    return parseSuccess;
  }

  public ResultSet getResultSet() throws OdpsException {
    return getCommandResultSet(null, null);
  }

  public ResultSet getResultSet(Long countLimit) throws OdpsException {
    return getCommandResultSet(null, countLimit);
  }

  public ResultSet getResultSet(Long offset, Long countLimit) throws OdpsException {
    return getCommandResultSet(offset, countLimit);
  }

  private ResultSet getCommandResultSet(Long offset, Long countLimit)
      throws OdpsException {
    if (offset != null && offset < 0L) {
      throw new IllegalArgumentException("illegal argument. offset = " + offset);
    }
    if (countLimit != null && countLimit < 0L) {
      throw new IllegalArgumentException("illegal argument. countLimit = " + countLimit);
    }

    Command command = commandInfo.getCommand();
    // 同步直接阻塞返回结果
    if (command.isSync()) {
      RecordIter recordIterator = command.run(odps, commandInfo);
      if (recordIterator == null) {
        return newEmptyResultSet();
      }
      recordIterator.setCountLimit(countLimit == null ? -1L : countLimit);
      recordIterator.setOffset(offset == null ? 0L : offset);
      TableSchema schema = new TableSchema();
      schema.setColumns(Arrays.asList(recordIterator.getColumns()));
      return new ResultSet(recordIterator, schema, -1);
    }
    Instance instance = commandInfo.getInstance();
    instance.waitForSuccess();
    instance = commandInfo.getInstance();
    String res = instance.getTaskResults().get(commandInfo.getTaskName());
    List<Record> records = CommandUtil.toRecord(res, command.getResultHeaders().get(0));
    TableSchema schema = new TableSchema();
    schema.setColumns(Arrays.asList(records.get(0).getColumns()));
    return new ResultSet(records.iterator(), schema, records.size());
  }

  public List<Record> getResult() throws OdpsException {
    return getCommandResult(null, null);
  }

  public List<Record> getResult(Long countLimit) throws OdpsException {
    return getCommandResult(null, countLimit);
  }

  public List<Record> getResult(Long offset, Long countLimit) throws OdpsException {
    return getCommandResult(offset, countLimit);
  }

  private List<Record> getCommandResult(Long offset, Long countLimit) throws OdpsException {
    if (offset != null && offset < 0L) {
      throw new IllegalArgumentException("illegal argument. offset = " + offset);
    }
    if (countLimit != null && countLimit < 0L) {
      throw new IllegalArgumentException("illegal argument. countLimit = " + countLimit);
    }
    Command command = commandInfo.getCommand();
    // 同步直接阻塞返回结果
    if (command.isSync()) {
      RecordIter recordIterator = command.run(odps, commandInfo);
      if (recordIterator == null) {
        return Collections.emptyList();
      }
      recordIterator.setCountLimit(countLimit == null ? -1L : countLimit);
      recordIterator.setOffset(offset == null ? 0L : offset);
      List<Record> records = new ArrayList<>();
      while (recordIterator.hasNext()) {
        Record record = recordIterator.next();
        records.add(record);
      }
      return records;
    }
    Instance instance = commandInfo.getInstance();
    instance.waitForSuccess();
    instance = commandInfo.getInstance();
    String res = instance.getTaskResults().get(commandInfo.getTaskName());

    return CommandUtil.toRecord(res, command.getResultHeaders().get(0));
  }

  private ResultSet newEmptyResultSet() {
    return new ResultSet(new EmptyRecordSetIterator(), new TableSchema(), 0);
  }

  class EmptyRecordSetIterator implements Iterator<Record> {

    public EmptyRecordSetIterator() {
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Record next() {
      return null;
    }
  }
}
