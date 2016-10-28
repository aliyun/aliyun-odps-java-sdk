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

package com.aliyun.odps.mapred.unittest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.CounterGroup;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordComparator;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.FileSplit;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.utils.ArchiveUtils;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.TaskId;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.CSVRecordReader;
import com.aliyun.odps.mapred.local.CSVRecordWriter;
import com.aliyun.odps.mapred.local.JobCounter;
import com.aliyun.odps.mapred.local.LocalTaskId;
import com.aliyun.odps.mapred.local.MRExceptionCode;
import com.aliyun.odps.mapred.local.MapDriver;
import com.aliyun.odps.mapred.local.ReduceDriver;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.TransformNode;
import com.aliyun.odps.utils.StringUtils;

/**
 * MapReduce 单元测试基类，方便用户编写 MapReduce 作业的单元测试用例.
 * 
 * <p>
 * MRUnitTest 提供两个主要的方法用于编写 Mapper 和 Reducer 的单元测试用例：
 * <ul>
 * <li>{@link #runMapper(JobConf, MapUTContext)} - 执行 Mapper 过程
 * <li>{@link #runReducer(JobConf, ReduceUTContext)} - 执行 Reducer 过程
 * </ul>
 * 
 * <p>
 * 另外，提供记录对象的创建、从文件读取、持久化到文件以及记录比较的一些常用方法。
 * 
 * <p>
 * 代码示例：
 * 
 * <pre>
 * package com.aliyun.odps.mapred.example;
 * 
 * import java.io.IOException;
 * import java.util.Iterator;
 * import java.util.List;
 * 
 * import junit.framework.Assert;
 * 
 * import org.junit.Test;
 * 
 * import com.aliyun.odps.data.Record;
 * import com.aliyun.odps.data.TableInfo;
 * import com.aliyun.odps.io.Text;
 * import com.aliyun.odps.mapred.MapperBase;
 * import com.aliyun.odps.mapred.ReducerBase;
 * import com.aliyun.odps.mapred.conf.JobConf;
 * import com.aliyun.odps.mapred.utils.InputUtils;
 * import com.aliyun.odps.mapred.utils.OutputUtils;
 * import com.aliyun.odps.mapred.utils.SchemaUtils;
 * import com.aliyun.odps.mapred.unittest.*;
 * 
 * public class WordCountTest extends MRUnitTest {
 * 
 *   private final static String INPUT_SCHEMA = &quot;a:string,b:string&quot;;
 *   private final static String OUTPUT_SCHEMA = &quot;k:string,v:bigint&quot;;
 * 
 *   &#064;Test
 *   public void TestMapReduce() throws IOException, ClassNotFoundException,
 *       InterruptedException {
 * 
 *     JobConf job = new JobConf();
 *     
 *     job.setMapperClass(TokenizerMapper.class);
 *     job.setCombinerClass(SumCombiner.class);
 *     job.setReducerClass(SumReducer.class);
 *     
 *     job.setMapOutputKeySchema(SchemaUtils.fromString(&quot;key:string&quot;));
 *     job.setMapOutputValueSchema(SchemaUtils.fromString(&quot;value:bigint&quot;));
 *     
 *     InputUtils.addTable(TableInfo.builder().tableName(&quot;wc_in&quot;).build(), job);
 *     OutputUtils.addTable(TableInfo.builder().tableName(&quot;wc_out&quot;).build(), job);
 * 
 *     // 准备 {@link Mapper} 的 MapUTContext 并指定输入输出 schema
 *     MapUTContext mapContext = new MapUTContext();
 *     mapContext.setInputSchema(INPUT_SCHEMA);
 *     mapContext.setOutputSchema(OUTPUT_SCHEMA);
 * 
 *     // 准备 {@link Mapper} 的输入记录
 *     Record record = mapContext.createInputRecord();
 *     record.set(new Text[] {new Text(&quot;hello&quot;), new Text(&quot;c&quot;)});
 *     mapContext.addInputRecord(record);
 *     record = mapContext.createInputRecord();
 *     record.set(new Text[] {new Text(&quot;hello&quot;), new Text(&quot;java&quot;)});
 *     mapContext.addInputRecord(record);
 * 
 *     // 开始执行 {@link Mapper} 过程
 *     TaskOutput mapOutput = runMapper(job, mapContext);
 * 
 *     // 验证 {@link Mapper}的执行结果
 *     List&lt;KeyValue&lt;Record, Record&gt;&gt; kvs = mapOutput.getOutputKeyValues();
 *     Assert.assertEquals(3, kvs.size());
 *     Assert.assertEquals(new KeyValue&lt;String, Long&gt;(new String(&quot;c&quot;), new Long(1)),
 *         new KeyValue&lt;String, Long&gt;((String) (kvs.get(0).getKey().get(0)), (Long) (kvs.get(0)
 *         .getValue().get(0))));
 *     Assert.assertEquals(new KeyValue&lt;String, Long&gt;(new String(&quot;hello&quot;), new Long(2)),
 *         new KeyValue&lt;String, Long&gt;((String) (kvs.get(1).getKey().get(0)), (Long) (kvs.get(1)
 *         .getValue().get(0))));
 *     Assert.assertEquals(new KeyValue&lt;String, Long&gt;(new String(&quot;java&quot;), new Long(1)),
 *         new KeyValue&lt;String, Long&gt;((String) (kvs.get(2).getKey().get(0)), (Long) (kvs.get(2)
 *         .getValue().get(0))));
 * 
 *     // 准备 {@link Reducer} 的输出 schema 和 输入键值对（Key/Value）
 *     ReduceUTContext reduceContext = new ReduceUTContext();
 *     reduceContext.setOutputSchema(OUTPUT_SCHEMA);
 * 
 *     // 准备 {@link Reducer} 的输入键值对，这里把 {@link Mapper} 的结果作为 {@link Reducer} 的输入
 *     reduceContext.addInputKeyValues(mapOutput);
 * 
 *     // 执行 {@link Reducer} 过程
 *     ReduceOutput output = runReducer(job, reduceContext);
 * 
 *     // 验证 {@link Reducer} 的执行结果
 *     List&lt;Record&gt; records = output.getOutputRecords();
 *     Assert.assertEquals(3, records.size());
 *     Assert.assertEquals(new String(&quot;c&quot;), records.get(0).get(&quot;k&quot;));
 *     Assert.assertEquals(new Long(1), records.get(0).get(&quot;v&quot;));
 *     Assert.assertEquals(new String(&quot;hello&quot;), records.get(1).get(&quot;k&quot;));
 *     Assert.assertEquals(new Long(2), records.get(1).get(&quot;v&quot;));
 *     Assert.assertEquals(new String(&quot;java&quot;), records.get(2).get(&quot;k&quot;));
 *     Assert.assertEquals(new Long(1), records.get(2).get(&quot;v&quot;));
 *   }
 * }
 * </pre>
 * 
 */
public class MRUnitTest {

  private static final Log LOG = LogFactory.getLog(MRUnitTest.class);
  private static Counters counters = new Counters();
  private static String DEFAULT_PROJECT_NAME = "default_mr_unittest";
  private static String DEFAULT_TABLE_NAME = "default_mr_unittest_table";

  /**
   * 执行 {@link Mapper} 过程.
   * 
   * <p>
   * 本方法根据给定的 {@link JobConf} 和 {@link MapUTContext} 执行 {@link Mapper} 过程。
   * 
   * @param job
   *          作业描述
   * @param context
   *          执行 {@link Mapper} 过程的上下文对象
   * @return {@link Mapper} 输出
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public TaskOutput runMapper(JobConf job, MapUTContext context)
      throws IOException, ClassNotFoundException, InterruptedException {
    Pipeline pipeline = Pipeline.fromJobConf(job);

    String jobId = generateMrTaskName();
    LOG.info("start to run mapper unittest, id: " + jobId);

    RuntimeContext ctx = RuntimeContext.create(jobId, job);

    TableInfo[] inputTableInfos = InputUtils.getTables(job);
    List<FileSplit> inputs = new ArrayList<FileSplit>();
    try {
      context.setRuntimeContext(ctx);

      // write job xml
      writeJobConf(job, ctx);

      // start to process input tables
      processInputs(job, inputs, context);
      
      // start to process resources
      processResources(job, context);

      int mapCopyNum = 0, reduceCopyNum = 0;
      if (inputTableInfos != null && inputTableInfos.length > 0) {
        mapCopyNum = inputTableInfos.length;
      } else {
        // allow no input
        mapCopyNum = job.getInt("odps.stage.mapper.num", 1);
      }

      TransformNode pipeNode = pipeline == null ? null : pipeline.getFirstNode();
      reduceCopyNum = computeReduceNum(mapCopyNum, pipeNode, job, ctx);

      TaskId taskId = new LocalTaskId("M1", 0, DEFAULT_PROJECT_NAME);
      TaskOutput mapOutput = pipeline == null ? new TaskOutput(job, reduceCopyNum) : new TaskOutput(job, pipeline, taskId.getTaskId(), reduceCopyNum);

      for (int mapId = 0; mapId < mapCopyNum; mapId++) {
        FileSplit split = inputs.size() > 0 ? inputs.get(mapId) : FileSplit.NullSplit;
        taskId = new TaskId("M1", mapId + 1);
        LOG.info("Start to run mapper, TaskId: " + taskId);

        BridgeJobConf conf = new BridgeJobConf(job);
        MapDriver mapDriver =
            new MapDriver(conf, split, taskId, mapOutput, counters, inputTableInfos == null ? null
                : inputTableInfos[mapId]);
        mapDriver.run();
      }
      return mapOutput;
    } finally {
      printCounters();
      // delete temp directory
      clean(context);
    }
  }

  private void processResources(JobConf job, UTContext context)
      throws IOException {

    RuntimeContext ctx = context.getRuntimeContext();

    // process files
    Map<String, byte[]> fileResources = context.getFileResources();
    for (Map.Entry<String, byte[]> entry : fileResources.entrySet()) {
      LOG.info("process file resource: " + entry.getKey());
      File resFile = new File(ctx.getResourceDir(), entry.getKey());
      FileUtils.writeByteArrayToFile(resFile, entry.getValue());
    }

    // process archives
    Map<String, File> archiveResources = context.getArchiveResources();
    for (Map.Entry<String, File> entry : archiveResources.entrySet()) {
      LOG.info("process archive resource: " + entry.getKey());
      File resDecompressedDir = new File(ctx.getResourceDir(), entry.getKey() + "_decompressed");
      File resFile = new File(ctx.getResourceDir(), entry.getKey());
      File path = entry.getValue();
      if (path.isFile()) {
        FileUtils.copyFile(path, resFile);
        ArchiveUtils.unArchive(resFile, resDecompressedDir);
      } else {
        resFile.createNewFile();
        FileUtils.copyDirectoryToDirectory(path, resDecompressedDir);
      }
    }

    // process tables
    Map<String, List<Record>> tableResources = context.getTableResources();
    Map<String, TableMeta> tableMetas = context.getTableMetas();
    for (Map.Entry<String, List<Record>> entry : tableResources.entrySet()) {
      LOG.info("process table resource: " + entry.getKey());
      File resDir = new File(ctx.getResourceDir(), entry.getKey());
      writeRecords(resDir, entry.getValue(), tableMetas.get(entry.getKey()));
    }

    context.clearResources();
  }

  private String generateMrTaskName() {
    return "mr_ut_" + LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_1).format(new Date());
  }

  private void processInputs(JobConf conf, List<FileSplit> inputs, MapUTContext context) throws IOException {
    TableInfo[] inputTableInfos = InputUtils.getTables(conf);

    if (inputTableInfos == null) {
      LOG.debug("No input tables to process");
      return;
    }

    for (TableInfo tableInfo : inputTableInfos) {
      LOG.debug("Start to process input table: " + tableInfo);
      if (StringUtils.isEmpty(tableInfo.getProjectName())) {
        tableInfo.setProjectName(DEFAULT_PROJECT_NAME);
      }
      processMapInputs(conf, inputs, context, tableInfo);
      LOG.debug("Finished process input table: " + tableInfo);

    }

    if (inputs.isEmpty()) {
      inputs.add(FileSplit.NullSplit);
    }
  }


  /**
   * 执行 {@link Reducer} 过程.
   * 
   * <p>
   * 本方法根据给定的 {@link JobConf} 和 {@link ReduceUTContext} 执行 {@link Reducer} 过程。
   * 
   * @param job
   *          作业描述
   * @param context
   *          执行 {@link Reducer} 过程的上下文对象
   * @return {@link Reducer} 输出
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public TaskOutput runReducer(JobConf job, ReduceUTContext context)
      throws IOException, ClassNotFoundException, InterruptedException {
    Pipeline pipeline = Pipeline.fromJobConf(job);

    String jobId = generateMrTaskName();
    LOG.info("start to run mapper unittest, id: " + jobId);

    RuntimeContext ctx = RuntimeContext.create(jobId, job);

    try {
      context.setRuntimeContext(ctx);

      // write job xml
      writeJobConf(job, ctx);

      int mapCopyNum = 1, reduceCopyNum = 0;
      TransformNode pipeNode = pipeline == null ? null : pipeline.getNode(context.getReducerIndex() + 1);
      int reduceIndx = pipeline == null ? 2 : (2+context.getReducerIndex());
      reduceCopyNum = computeReduceNum(mapCopyNum, pipeNode, job, ctx);

      // construct reducer's input
      TaskId preTaskId = new LocalTaskId(reduceIndx == 2 ? "M1" : "R"+Integer.toString(reduceIndx-1), 0, DEFAULT_PROJECT_NAME);
      TaskOutput reduceInput = pipeline == null ? new TaskOutput(job, reduceCopyNum) : new TaskOutput(job, pipeline, preTaskId.getTaskId(), reduceCopyNum);
      for (KeyValue<Record, Record> kv : context.getInputKeyVals()) {
        reduceInput.add(kv.getKey(), kv.getValue());
      }

      // start to process resources
      processResources(job, context);

      TaskId taskId = new LocalTaskId("R"+Integer.toString(reduceIndx), 0, DEFAULT_PROJECT_NAME);
      TaskOutput reduceOutput = pipeline == null ? new TaskOutput(job, reduceCopyNum) : new TaskOutput(job, pipeline, taskId.getTaskId(), reduceCopyNum);;
      for (int reduceId = 0; reduceId < reduceCopyNum; ++reduceId) {
        taskId = new TaskId("R"+Integer.toString(reduceIndx), reduceId);
        LOG.info("Start to run reduce, taskId: " + taskId);

        BridgeJobConf conf = new BridgeJobConf(job);
        ReduceDriver reduceDriver =
            new ReduceDriver(conf, reduceInput, reduceOutput, taskId, counters, reduceId);
        reduceDriver.run();

        LOG.info("Finished run reduce, taskId: " + taskId);
      }

      return reduceOutput;

    } finally {
      printCounters();
      // delete temp directory
      clean(context);
    }
  }

  /**
   * 给定 schema 创建记录对象（{@link Record}）.
   * 
   * <p>
   * schema 的格式是：(<列名>:<类型>)(,<列名>:<类型>)+ <br/>
   * 例如：a:string,b:string
   * 
   * @param schema
   *          待创建记录对象的 schema
   * @return 记录对象
   * @throws IOException
   */
  public static Record createRecord(String schema) throws IOException {
    return new WritableRecord(com.aliyun.odps.mapred.utils.SchemaUtils.fromString(schema));
  }

  /**
   * 从给定的本地目录读取记录.
   * 
   * <p>
   * 目录需要包含一个名为“__schema__”的文件描述表的
   * schema，格式为：[<proj>.<table_name>,]<col_name>:<col_type
   * >(,<col_name>:<col_type>)*<br />
   * 例如：
   * <ul>
   * <li>proj.tablename,a:string,b:bigint,c:double
   * <li>a:string,b:bigint,c:double
   * </ul>
   * 
   * @param dir
   *          本地目录
   * @return 记录对象 {@link List}
   * @throws IOException
   * 
   * @see {@link #writeRecords(File, List, String)}
   */
  public static List<Record> readRecords(File dir) throws IOException {
    List<Record> records = new ArrayList<Record>();
    TableMeta meta = SchemaUtils.readSchema(dir);
    File dataFile = new File(dir, "data");
    if (!dataFile.exists()) {
      return records;
    }
    Counter emptyCounter = counters.findCounter(JobCounter.__EMPTY_WILL_NOT_SHOW);
    RecordReader reader =
        new CSVRecordReader(new FileSplit(dataFile, meta.getCols(), 0, dataFile.getTotalSpace()),
            meta, emptyCounter, emptyCounter, counters,
            WareHouse.getInstance().getInputColumnSeperator());

    Record r = reader.read();
    while (r != null) {
      records.add(r.clone());
      r = reader.read();
    }
    reader.close();

    return records;
  }

  /**
   * 将记录写到给定的本地目录.
   * 
   * <p>
   * 需要给一个 schema，格式为：[<proj>.<table_name>,]<col_name>:<col_type
   * >(,<col_name>:<col_type>)*<br />
   * 例如：
   * <ul>
   * <li>proj.tablename,a:string,b:bigint,c:double
   * <li>a:string,b:bigint,c:double
   * </ul>
   * schema 会输出到给定目录下的一个名为“__schema__”的文件。
   * 
   * @param dir
   *          本地目录
   * @param records
   *          待输出的记录对象 {@link List}
   * @param schema
   *          记录的 schema
   * @throws IOException
   * 
   * @see {@link #readRecords(File)}
   */
  public static void writeRecords(File dir, List<Record> records, TableMeta meta)
      throws IOException {
    if (StringUtils.isEmpty(meta.getProjName())) {
      meta.setProjName(DEFAULT_PROJECT_NAME);
    }
    if (StringUtils.isEmpty(meta.getTableName())) {
      meta.setTableName(DEFAULT_TABLE_NAME);
    }

    dir.mkdirs();
    SchemaUtils.generateSchemaFile(meta, null, dir);
    File file = new File(dir, "data");
    file.createNewFile();
    RecordWriter writer =
        new CSVRecordWriter(file, null, null, RuntimeContext.getWareHouse()
            .getInputColumnSeperator());

    for (Record record : records) {
      writer.write(record);
    }
    writer.close();
  }

  /**
   * 比较两个记录列表是否相同（相等），记录比较见 {@link RecordComparator}.
   * 
   * @param records1
   *          记录列表1
   * @param records2
   *          记录列表2
   * @param sort
   *          是否先对列表排序后再比较
   * @return 记录列表相等返回true，否则返回false
   */
  public static boolean equalRecords(List<Record> records1,
      List<Record> records2, boolean sort) {
    List<Record> list1 = new ArrayList<Record>(records1);
    List<Record> list2 = new ArrayList<Record>(records2);
    LocalRecordComparator comparator = new LocalRecordComparator();
    if (sort) {
      Collections.sort(list1, comparator);
      Collections.sort(list2, comparator);
    }
    boolean comp = (list1.size() == records2.size());
    for (int i = 0; i < list1.size() && comp; i++) {
      comp = (comparator.compare(list1.get(i), list2.get(i)) == 0);
    }
    return comp;
  }

  /**
   * 比较给定本地目录下的记录集合和一个记录列表是否相同（相等），记录比较见 {@link RecordComparator}.
   * 
   * <p>
   * 此方法等价于使用 {@link #readRecords(File)} 读取本地目录的记录对象到一个记录列表，然后再使用
   * {@link #equalRecords(List, List, boolean)} 进行比较。
   * 
   * @param dir
   *          本地目录
   * @param records
   *          记录列表
   * @param sort
   *          是否先对列表排序后再比较
   * @return
   * @throws IOException
   */
  public static boolean equalRecords(File dir, List<Record> records,
      boolean sort) throws IOException {
    List<Record> list = readRecords(dir);
    return equalRecords(list, records, sort);
  }

  private void processMapInputs(JobConf job, List<FileSplit> inputs, MapUTContext context,
      TableInfo tblInfo) throws IOException {
    LOG.info("process map input: " + tblInfo);

    RuntimeContext ctx = context.getRuntimeContext();

    String projName = tblInfo.getProjectName();
    String tblName = tblInfo.getTableName();
    String fullName = projName + "." + tblName;
    String partSpec = tblInfo.getPartPath();

    String s = context.getInputSchema();
    if (s == null) {
      throw new IOException("input schema is not set.");
    }
    Column[] cols = com.aliyun.odps.mapred.utils.SchemaUtils.fromString(s.trim());
    TableMeta meta = new TableMeta(projName, tblName, cols, null);
    File tblDir = ctx.getInputDir(fullName, partSpec);
    prepareTableDir(meta, inputs, tblDir, context.getInputRecords());
  }

  private void prepareTableDir(TableMeta meta, List<FileSplit> inputs, File tblDir,
      List<Record> records) throws IOException {
    String tbl = meta.getProjName() + "." + meta.getTableName();
    LOG.info("prepare table dir: " + tbl + " to "
        + tblDir.getAbsolutePath());

    writeRecords(tblDir, records, meta);
    
    File file = new File(tblDir, "data");
    FileSplit split = new FileSplit(file, meta.getCols(), 0L, file.length());
    inputs.add(split);
  }

  private void clean(UTContext context) throws IOException {
    if (context.isCleanUtDir()) {
      FileUtils.deleteDirectory(context.runtimeContext.getJobDir());
    }
  }

  private void writeJobConf(JobConf job, RuntimeContext ctx) throws IOException {
    // write job configuration to temporary file job.xml
    FileOutputStream fos;
    fos = new FileOutputStream(ctx.getJobFile());
    job.writeXml(fos);
    fos.close();
  }

  private int computeReduceNum(int mapNum, TransformNode pipeNode, JobConf conf, RuntimeContext ctx) throws IOException {
    int reduceNum = 1;
    if (pipeNode != null) {
      if (pipeNode.getNextNode() != null) {
        reduceNum = pipeNode.getNextNode().getNumTasks();
      } else {
        reduceNum = pipeNode.getNumTasks();
      }
    } else if (ctx.containsKey("odps.stage.reducer.num")) {
      reduceNum = conf.getNumReduceTasks();
    } else {
      reduceNum = Math.max(1, mapNum / 4);
    }
    if (reduceNum < 0) {
      throw new IOException(MRExceptionCode.ODPS_0720251 + " - reduce num cann't be less than 0");
    } else if (reduceNum != conf.getNumReduceTasks()) {
      LOG.info("change reduce num from " + conf.getNumReduceTasks() + " to " + reduceNum);
    }
    conf.setNumReduceTasks(reduceNum);
    return reduceNum;
  }

  private void printCounters() {
    int totalCount = 0;
    int frameWorkCounterCount = 0;
    int jobCounterCount = 0;
    int userCounterCount = 0;
    for (CounterGroup group : counters) {
      for (Counter counter : group) {
        if (counter.getDisplayName().startsWith("__EMPTY_")) {
          continue;
        } else if (group.getDisplayName().equals(JobCounter.class.getName())) {
          ++frameWorkCounterCount;
        } else if (group.getDisplayName()
            .equals("com.aliyun.odps.mapred.local.Counter.JobCounter")) {
          ++jobCounterCount;
        } else {
          ++userCounterCount;
        }

        ++totalCount;

      }
    }

    StringBuilder sb = new StringBuilder("Counters: " + totalCount);
    sb.append("\n\tMap-Reduce Framework: " + frameWorkCounterCount);
    for (CounterGroup group : counters) {
      if (!group.getDisplayName().equals(JobCounter.class.getName())) {
        continue;
      }
      for (Counter counter : group) {
        if (counter.getDisplayName().startsWith("__EMPTY_")) {
          continue;
        }
        sb.append("\n\t\t" + counter.getDisplayName() + "=" + counter.getValue());
      }
    }

    // sb.append("\n\tJob Counters: " + jobCounterCount);
    // for (CounterGroup group : counters) {
    // if
    // (!group.getDisplayName().equals("com.aliyun.odps.mapred.local.Counter.JobCounter"))
    // {
    // continue;
    // }
    // for (Counter counter : group) {
    // if
    // (counter.getDisplayName().equals(JobCounter.__EMPTY_WILL_NOT_SHOW.toString()))
    // continue;
    // sb.append("\n\t\t" + counter.getDisplayName() + "=" +
    // counter.getValue());
    // }
    // }

    sb.append("\n\tUser Defined Counters: " + userCounterCount);
    for (CounterGroup group : counters) {
      if (group.getDisplayName().equals(JobCounter.class.getName())
          || group.getDisplayName().equals("com.aliyun.odps.mapred.local.Counter.JobCounter")) {
        continue;
      }
      sb.append("\n\t\t" + group.getDisplayName());
      for (Counter counter : group) {
        if (counter.getDisplayName().equals(JobCounter.__EMPTY_WILL_NOT_SHOW.toString())) {
          continue;
        }
        sb.append("\n\t\t\t" + counter.getDisplayName() + "=" + counter.getValue());
      }
    }
    System.err.println(sb.toString().toLowerCase());
  }
}
