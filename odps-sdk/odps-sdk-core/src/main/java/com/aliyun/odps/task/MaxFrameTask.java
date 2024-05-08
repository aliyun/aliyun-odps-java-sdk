package com.aliyun.odps.task;

import com.aliyun.odps.Task;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;

/**
 * Created by Wenjun Si on 24-4-22.
 */
@Root(name = "MaxFrame", strict = false)
public class MaxFrameTask extends Task {

  @Element(name = "Command")
  private String command;

  // Required by SimpleXML
  MaxFrameTask() {
  }

  /**
   * 使用给定任务名构造一个{@link MaxFrameTask}实例。
   *
   * @param name
   *     任务名。
   * @param command
   *     命令名。
   */
  public MaxFrameTask(String name, String command) {
    super();
    setName(name);
    setCommand(command);
  }

  public String getCommand() {
    return this.command;
  }

  public void setCommand(String value) {
    this.command = value;
  }
}
