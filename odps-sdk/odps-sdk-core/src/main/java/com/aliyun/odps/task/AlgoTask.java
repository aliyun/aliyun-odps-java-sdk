package com.aliyun.odps.task;

import com.aliyun.odps.Task;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

/**
 * Created by ruibo.lirb on 2016-12-06.
 */
@Root(name = "AlgoTask", strict = false)
public class AlgoTask extends Task {

  AlgoTask() {}

  @Element(name = "TaskPlan", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  private String taskPlan;

  public void setTaskPlan(String taskPlan) {
    this.taskPlan = taskPlan;
  }

  public String getTaskPlan() {
    return taskPlan;
  }

}
