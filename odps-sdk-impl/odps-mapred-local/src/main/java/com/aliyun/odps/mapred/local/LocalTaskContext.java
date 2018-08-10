/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.mapred.local;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.FileSplit;
import com.aliyun.odps.local.common.JobDirecotry;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.aliyun.odps.local.common.utils.ArchiveUtils;
import com.aliyun.odps.mapred.LocalJobRunner;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;
import com.aliyun.odps.utils.ReflectionUtils;

public abstract class LocalTaskContext implements TaskContext {

  public static final Log LOG = LogFactory.getLog(LocalTaskContext.class);

  protected BridgeJobConf conf;
  private TaskId taskId;

  private final Counters counters;

  protected Map<String, RecordWriter> recordWriters;

  // pipeline mode 
  protected Pipeline pipeline;
  protected boolean pipeMode; // use pipeline mapreduce
  protected int pipeIndex = -1;
  protected TransformNode pipeNode;
  private JobDirecotry jobDirecotry;

  protected int reducerNum = 0;


  public LocalTaskContext(BridgeJobConf conf, TaskId taskid, Counters counters)
      throws IOException {
    this.conf = conf;
    this.taskId = taskid;
    this.jobDirecotry = new JobDirecotry();

    this.pipeline = Pipeline.fromJobConf(conf);
    if (this.pipeline != null) {
      this.pipeMode = true;
      String taskId = getTaskID().toString();
      System.err.println("Task ID: " + taskId);
      this.pipeIndex = Integer.parseInt(taskId.split("_")[0].substring(1)) - 1;
      this.pipeNode = pipeline.getNode(pipeIndex);
    }

    if (pipeMode && pipeNode != null) {
      if (pipeNode.getNextNode() != null) {
        reducerNum = pipeNode.getNextNode().getNumTasks();
      } else if (pipeIndex > 0) {
        // the last but not the first node of pipeline, must be reduce node
        reducerNum = pipeNode.getNumTasks();
      } else {
        reducerNum = 0;
      }
    } else {
      reducerNum = conf.getNumReduceTasks();
    }

    this.recordWriters = new HashMap<String, RecordWriter>();
    TableInfo[] output = OutputUtils.getTables(conf);
    if (output != null) {
      for (TableInfo info : output) {
        Counter recordCounter;
        Counter byteCounter;
        int reduceNum = conf.getNumReduceTasks();
        if (taskid.isMap() && reduceNum > 0) {
          // since the map output is a buffer,will not write to file
          // this counter will not change,but can't be null
          Counter emptyCounter = counters.findCounter(JobCounter.__EMPTY_WILL_NOT_SHOW);
          recordCounter = emptyCounter;
          byteCounter = emptyCounter;
        } else {
          recordCounter = counters.findCounter(JobCounter.class.getName(),
                                               String.format("%s_OUTPUT_[%s]_RECORDS",
                                                             taskId.isMap() ? "MAP" : "REDUCE",
                                                             info));
          byteCounter = counters.findCounter(JobCounter.class.getName(),
                                             String.format("%s_OUTPUT_[%s]_BYTES",
                                                           taskId.isMap() ? "MAP" : "REDUCE",
                                                           info));
        }
        RecordWriter
            writer =
            new CSVRecordWriter(new File(jobDirecotry.getOutputDir(info.getLabel()),
                                         taskId.toString()), recordCounter, byteCounter,
                                WareHouse.getInstance().getInputColumnSeperator());
        recordWriters.put(info.getLabel(), writer);
      }
    }

    this.counters = counters;

  }

  public Mapper createMapper() {
    return ReflectionUtils.newInstance(getMapperClass(), conf);
  }

  public Reducer createReducer() {
    return ReflectionUtils.newInstance(getReducerClass(), conf);
  }

  public Reducer createCombiner() {
    if (getCombinerClass() != null) {
      return ReflectionUtils.newInstance(getCombinerClass(), conf);
    }
    return null;
  }

  public void closeWriters() throws IOException {
    for (RecordWriter writer : recordWriters.values()) {
      writer.close();
    }
  }

  @Override
  public String[] getGroupingColumns() {
    if (pipeMode && pipeNode != null) {
      return pipeNode.getOutputGroupingColumns();
    } else {
      return conf.getOutputGroupingColumns();
    }
  }

  @Override
  public Column[] getMapOutputKeySchema() {
    return conf.getMapOutputKeySchema();
  }

  @Override
  public Column[] getMapOutputValueSchema() {
    return conf.getMapOutputValueSchema();
  }

  @Override
  public Class<? extends Mapper> getMapperClass() {
    return conf.getMapperClass();
  }

  @Override
  public int getNumReduceTasks() {
    return reducerNum;
  }

  @Override
  public Class<? extends Reducer> getReducerClass() {
    return conf.getReducerClass();
  }

  @Override
  public Class<? extends Reducer> getCombinerClass() {
    return conf.getCombinerClass();
  }

  @Override
  public Record createOutputRecord() throws IOException {
    return createOutputRecord(TableInfo.DEFAULT_LABEL);
  }

  @Override
  public Record createOutputRecord(String label) throws IOException {
    return new WritableRecord(conf.getOutputSchema(label));
  }

  @Override
  public Counter getCounter(Enum<?> key) {
    return counters.findCounter(key);
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return counters.findCounter(groupName, counterName);
  }

  @Override
  public TaskId getTaskID() {
    return taskId;
  }

  @Override
  public void progress() {
    // do nothing
  }

  @Override
  public BufferedInputStream readResourceFileAsStream(String name)
      throws IOException {
    if (StringUtils.isEmpty(name)) {
      throw new IOException("Resouce name is empty or null");
    }

    if (!jobDirecotry.hasResource(name)) {
      String project = SessionState.get().getOdps().getDefaultProject();
      try {
        WareHouse.getInstance().copyResource(project, name,
                                             jobDirecotry.getResourceDir(),
                                             WareHouse.getInstance().getLimitDownloadRecordCount(),
                                             WareHouse.getInstance().getInputColumnSeperator());
      } catch (OdpsException e) {
      }
    }
    File file = new File(jobDirecotry.getResourceDir(), name);
    return new BufferedInputStream(new FileInputStream(file));
  }

  private static class InputStreamIterator implements Iterator<BufferedInputStream> {

    private Iterator<File> files;

    public InputStreamIterator(Iterator<File> files) {
      super();
      this.files = files;
    }

    @Override
    public boolean hasNext() {
      return files.hasNext();
    }

    @Override
    public BufferedInputStream next() {
      File file = files.next();
      try {
        return new BufferedInputStream(new FileInputStream(file));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void remove() {
      throw new RuntimeException("remove Unsupported");
    }

  }

  @Override
  public Iterable<BufferedInputStream> readResourceArchiveAsStream(String resourceName)
      throws IOException {
    return readResourceArchiveAsStream(resourceName, "");
  }

  @Override
  public Iterable<BufferedInputStream> readResourceArchiveAsStream(String resourceName,
      String relativePath) throws IOException {
    if (StringUtils.isEmpty(resourceName)) {
      throw new IOException("Resouce name is empty or null");
    }

    File resFile = new File(jobDirecotry.getResourceDir(), resourceName);;
    if (!jobDirecotry.hasResource(resourceName)) {
      String project = SessionState.get().getOdps().getDefaultProject();
      try {
        WareHouse.getInstance().copyResource(project, resourceName,
                                             jobDirecotry.getResourceDir(),
                                             WareHouse.getInstance().getLimitDownloadRecordCount(),
                                             WareHouse.getInstance().getInputColumnSeperator());
      } catch (OdpsException e) {
      }
    }

    File resDir = new File(jobDirecotry.getResourceDir(), resourceName + "_decompressed");;
    if (!resDir.exists()) {
      ArchiveUtils.unArchive(resFile, resDir);
    }

    final Collection<File> files = LocalRunUtils.listFiles(resDir, relativePath.trim());
    return new Iterable<BufferedInputStream>() {
      @Override
      public Iterator<BufferedInputStream> iterator() {
        return new InputStreamIterator(files.iterator());
      }
    };
  }

  @Override
  public Iterator<Record> readResourceTable(String tbl) throws IOException {
    if (StringUtils.isEmpty(tbl)) {
      throw new IOException("Table resouce name is empty or null");
    }

    if (!jobDirecotry.hasResource(tbl)) {
      String project = SessionState.get().getOdps().getDefaultProject();
      try {
        WareHouse.getInstance().copyResource(project, tbl, jobDirecotry.getResourceDir(),
                                             WareHouse.getInstance().getLimitDownloadRecordCount(),
                                             WareHouse.getInstance().getInputColumnSeperator());
      } catch (OdpsException e) {
      }
    }

    File dir = new File(jobDirecotry.getResourceDir(), tbl);
    LOG.info("Reading resource table from " + dir);
    final List<File> datafiles = new ArrayList<File>();

    LocalRunUtils.listAllDataFiles(dir, datafiles);

    final TableMeta tableMeta = SchemaUtils.readSchema(dir);

    return new Iterator<Record>() {
      RecordReader reader;
      Record current;
      boolean fetched;

      @Override
      public boolean hasNext() {
        if (fetched) {
          return current != null;
        }
        // Fetch new one
        try {
          fetch();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return current != null;

      }

      private void fetch() throws IOException {

        // first time
        if (reader == null) {
          if (datafiles.isEmpty()) {
            current = null;
            fetched = true;
            return;
          }

          File f = datafiles.remove(0);
          reader =
              new CSVRecordReader(new FileSplit(f, tableMeta.getCols(), 0, f.getTotalSpace()),
                  tableMeta, LocalJobRunner.EMPTY_COUNTER, LocalJobRunner.EMPTY_COUNTER, counters,
                  WareHouse.getInstance().getInputColumnSeperator());
          current = reader.read();
          fetched = true;
          return;
        }

        current = reader.read();
        if (current == null && !datafiles.isEmpty()) {
          File f = datafiles.remove(0);
          reader =
              new CSVRecordReader(new FileSplit(f, tableMeta.getCols(), 0, f.getTotalSpace()),
                  tableMeta, LocalJobRunner.EMPTY_COUNTER, LocalJobRunner.EMPTY_COUNTER, counters,
                  WareHouse.getInstance().getInputColumnSeperator());
          current = reader.read();
          fetched = true;
          return;
        }

        fetched = true;
      }

      @Override
      public Record next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        fetched = false;
        return current.clone();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

    };
  }

  @Override
  public JobConf getJobConf() {
    return (JobConf) conf;
  }

  @Override
  public Record createMapOutputKeyRecord() {
    if (pipeMode && pipeNode != null && pipeNode.getType().equals("map")) {
      return new WritableRecord(pipeNode.getOutputKeySchema());
    } else {
      return new WritableRecord(conf.getMapOutputKeySchema());
    }
  }

  @Override
  public Record createMapOutputValueRecord() {
    if (pipeMode && pipeNode != null && pipeNode.getType().equals("map")) {
      return new WritableRecord(pipeNode.getOutputValueSchema());
    } else {
      return new WritableRecord(conf.getMapOutputValueSchema());
    }
  }

  @Override
  public TableInfo[] getOutputTableInfo() {
    return OutputUtils.getTables(conf);
  }

  @Override
  public Record createOutputKeyRecord() throws IOException {
    if (pipeMode && pipeNode != null) {
      return new WritableRecord(pipeNode.getOutputKeySchema());
    } else {
      return null;
    }
  }

  @Override
  public Record createOutputValueRecord() throws IOException {
    if (pipeMode && pipeNode != null) {
      return new WritableRecord(pipeNode.getOutputValueSchema());
    } else {
      return null;
    }
  }

  public boolean isPipelineMode() {
    return this.pipeMode;
  }

  public Pipeline getPipeline() {
    return this.pipeline;
  }

  public TransformNode getCurrentNode() {
    return this.pipeNode;
  }
}
