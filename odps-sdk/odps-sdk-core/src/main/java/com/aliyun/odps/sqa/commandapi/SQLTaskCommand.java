package com.aliyun.odps.sqa.commandapi;

import java.util.Calendar;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;

abstract class SQLTaskCommand implements Command {

  @Override
  public boolean isSync() {
    return false;
  }

  RecordIter sqlTaskRun(Odps odps, CommandInfo commandInfo, String taskNamePrefix)
      throws OdpsException {
    String sql = commandInfo.getCommandText();
    String taskName = taskNamePrefix + Calendar.getInstance().getTimeInMillis();

    String project = odps.getDefaultProject();
    if (project == null) {
      throw new OdpsException("default project required.");
    }

    Instance instance = SQLTask.run(odps, project, sql, taskName, commandInfo.getHint(), null);
    commandInfo.setTaskName(taskName);
    commandInfo.setInstance(instance, odps.logview().generateLogView(instance, 7 * 24), null);

    return null;
  }
}
