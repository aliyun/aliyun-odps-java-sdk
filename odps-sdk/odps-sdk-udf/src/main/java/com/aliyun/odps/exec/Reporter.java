package com.aliyun.odps.exec;

import com.aliyun.odps.counter.Counter;

/**
 *
 */
public interface Reporter {

  /**
   * Set the status description for the task.
   *
   * @param status brief description of the current status.
   */
  void setStatus(String status);

  /**
   * Get the {@link Counter} of the given group with the given name.
   *
   * @param name counter name
   * @return the <code>Counter</code> of the given group/name.
   */
  Counter getCounter(Enum<?> name);

  /**
   * Get the {@link Counter} of the given group with the given name.
   *
   * @param group counter group
   * @param name counter name
   * @return the <code>Counter</code> of the given group/name.
   */
  Counter getCounter(String group, String name);

  /**
   * Increments the counter identified by the key, which can be of
   * any {@link Enum} type, by the specified amount.
   *
   * @param key key to identify the counter to be incremented. The key can be
   *            be any <code>Enum</code>.
   * @param amount A non-negative amount by which the counter is to
   *               be incremented.
   */
  void incrCounter(Enum<?> key, long amount);

  /**
   * Increments the counter identified by the group and counter name
   * by the specified amount.
   *
   * @param group name to identify the group of the counter to be incremented.
   * @param counter name to identify the counter within the group.
   * @param amount A non-negative amount by which the counter is to
   *               be incremented.
   */
  void incrCounter(String group, String counter, long amount);

  /**
   * Get the {@link InputSplit} object for a map.
   *
   * @return the <code>InputSplit</code> that the map is reading from.
   * @throws UnsupportedOperationException if called outside a mapper
   */
  InputSplit getInputSplit()
          throws UnsupportedOperationException;

  /**
   * Get the progress of the task. Progress is represented as a number between
   * 0 and 1 (inclusive).
   */
  float getProgress();

  /**
   * Report progress to the framework.
   */
  public void progress();
}
