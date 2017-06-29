package com.aliyun.odps.task;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.aliyun.odps.Task;

/**
 * Created by ruibo.lirb on 2016-12-06.
 */
@XmlRootElement(name = "AlgoTask")
public class AlgoTask extends Task {

  AlgoTask() {}

  private String taskPlan;

  @XmlElement(name = "TaskPlan")
  public void setTaskPlan(String taskPlan) {
    this.taskPlan = taskPlan;
  }

  public String getTaskPlan() {
    return taskPlan;
  }

}
