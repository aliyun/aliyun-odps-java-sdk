/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mapred.bridge.streaming;

import static com.aliyun.odps.mapred.utils.UTF8ByteArrayUtils.unescapeSeparator;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSON;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Resource;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.bridge.MetaExplorer;
import com.aliyun.odps.mapred.bridge.MetaExplorerImpl;
import com.aliyun.odps.mapred.bridge.streaming.io.InputWriter;
import com.aliyun.odps.mapred.bridge.streaming.io.OutputReader;
import com.aliyun.odps.mapred.bridge.streaming.io.RecordInputWriter;
import com.aliyun.odps.mapred.bridge.streaming.io.RecordOutputReader;
import com.aliyun.odps.mapred.bridge.streaming.io.TextInputWriter;
import com.aliyun.odps.mapred.bridge.streaming.io.TextOutputReader;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

/**
 * All the client-side work happens here.
 * (Jar packaging, MapRed job submission and monitoring)
 */
public class StreamJob {

  protected static final Log LOG = LogFactory.getLog(StreamJob.class.getName());
  final static String REDUCE_NONE = "NONE";

  /**
   * -----------Streaming CLI Implementation  *
   */
  private CommandLineParser parser = new BasicParser();
  private Options allOptions;

  public StreamJob() {
    setupOptions();
    this.config_ = new JobConf();
  }

  public Configuration getConf() {
    return config_;
  }

  public int run(String[] args) throws Exception {
    for (String aa : args) {
      LOG.debug("arg: '" + aa + "'");
    }
    try {
      this.argv_ = Arrays.copyOf(args, args.length);
      init();

      preProcessArgs();
      parseArgv();
      if (printUsage) {
        printUsage(detailedUsage_);
        return 0;
      }
      postProcessArgs();

      setJobConf();
    } catch (IllegalArgumentException ex) {
      //ignore, since log will already be printed
      // print the log in debug mode.
      LOG.debug("Error in streaming job", ex);
      ex.printStackTrace();
      return 1;
    }
    return submitAndMonitorJob();
  }

  /**
   * This method creates a streaming job from the given argument list.
   * The created object can be used and/or submitted to a jobtracker for
   * execution by a job agent such as JobControl
   *
   * @param argv
   *     the list args for creating a streaming job
   * @return the created JobConf object
   * @throws IOException
   */
  static public JobConf createJob(String[] argv) throws IOException {
    StreamJob job = new StreamJob();
    job.argv_ = argv;
    job.init();
    job.preProcessArgs();
    job.parseArgv();
    job.postProcessArgs();
    job.setJobConf();
    return job.jobConf_;
  }

  protected void init() {
    try {
      env_ = new Environment();
    } catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  void preProcessArgs() {
    verbose_ = false;
    // Unset HADOOP_ROOT_LOGGER in case streaming job
    // invokes additional hadoop commands.
    addTaskEnvironment_ = "HADOOP_ROOT_LOGGER=";
  }

  void postProcessArgs() throws IOException {

    msg("addTaskEnvironment=" + addTaskEnvironment_);

    for (final String packageFile : packageFiles_) {
      File f = new File(packageFile);
      if (f.isFile()) {
        shippedCanonFiles_.add(f.getCanonicalPath());
      }
    }
    msg("shippedCanonFiles_=" + shippedCanonFiles_);

    // careful with class names..
    mapCmd_ = unqualifyIfLocalPath(mapCmd_);
    comCmd_ = unqualifyIfLocalPath(comCmd_);
    redCmd_ = unqualifyIfLocalPath(redCmd_);
  }

  String unqualifyIfLocalPath(String cmd) throws IOException {
    if (cmd == null) {
      //
    } else {
      String prog = cmd;
      String args = "";
      int s = cmd.indexOf(" ");
      if (s != -1) {
        prog = cmd.substring(0, s);
        args = cmd.substring(s + 1);
      }
      String progCanon;
      try {
        progCanon = new File(prog).getCanonicalPath();
      } catch (IOException io) {
        progCanon = prog;
      }
      boolean shipped = shippedCanonFiles_.contains(progCanon);
      msg("shipped: " + shipped + " " + progCanon);
      if (shipped) {
        // Change path to simple filename.
        // That way when PipeMapRed calls Runtime.exec(),
        // it will look for the excutable in Task's working dir.
        // And this is where TaskRunner unjars our job jar.
        prog = new File(prog).getName();
        if (args.length() > 0) {
          cmd = prog + " " + args;
        } else {
          cmd = prog;
        }
      }
    }
    msg("cmd=" + cmd);
    return cmd;
  }

  void parseArgv() {
    CommandLine cmdLine = null;
    try {
      cmdLine = parser.parse(allOptions, argv_);
    } catch (Exception oe) {
      LOG.error(oe.getMessage());
      exitUsage(argv_.length > 0 && "-info".equals(argv_[0]));
    }

    if (cmdLine == null) {
      exitUsage(argv_.length > 0 && "-info".equals(argv_[0]));
      return;
    }

    @SuppressWarnings("unchecked")
    List<String> args = cmdLine.getArgList();
    if (args != null && args.size() > 0) {
      fail("Found " + args.size() + " unexpected arguments on the " +
           "command line " + args);
    }

    detailedUsage_ = cmdLine.hasOption("info");
    if (cmdLine.hasOption("help") || detailedUsage_) {
      printUsage = true;
      return;
    }
    verbose_ = cmdLine.hasOption("verbose");
    background_ = cmdLine.hasOption("background");
    debug_ = cmdLine.hasOption("debug") ? debug_ + 1 : debug_;

    output_ = cmdLine.getOptionValue("output");

    comCmd_ = cmdLine.getOptionValue("combiner");
    redCmd_ = cmdLine.getOptionValue("reducer");

    lazyOutput_ = cmdLine.hasOption("lazyOutput");

    String[] values = cmdLine.getOptionValues("file");
    SessionState ss = SessionState.get();
    MetaExplorer metaExplorer = new MetaExplorerImpl(ss.getOdps());
    Map<String, String> aliasToTempResource = new HashMap<String, String>();
    String padding = "_" + UUID.randomUUID().toString();
    if (values != null && values.length > 0) {
      for (int i = 0; i < values.length; i++) {
        String file = values[i];
        packageFiles_.add(file);
        try {
          aliasToTempResource.put(FilenameUtils.getName(file),
                                  metaExplorer.addFileResourceWithRetry(file, Resource.Type.FILE,
                                                                        padding, true));
        } catch (OdpsException e) {
          throw new RuntimeException(e);
        }
      }

      config_.set("stream.temp.resource.alias", JSON.toJSONString(aliasToTempResource));

      String[] res = config_.getResources();
      Set<String> resources = aliasToTempResource.keySet();
      if (res != null) {
        config_.setResources(StringUtils.join(res, ",") + "," + StringUtils.join(resources, ","));
      } else {
        config_.setResources(StringUtils.join(resources, ","));
      }
    }

    additionalConfSpec_ = cmdLine.getOptionValue("additionalconfspec");
    numReduceTasksSpec_ = cmdLine.getOptionValue("numReduceTasks");
    partitionerSpec_ = cmdLine.getOptionValue("partitioner");
    mapDebugSpec_ = cmdLine.getOptionValue("mapdebug");
    reduceDebugSpec_ = cmdLine.getOptionValue("reducedebug");
    ioSpec_ = cmdLine.getOptionValue("io");

    String[] car = cmdLine.getOptionValues("cacheArchive");
    if (null != car) {
      fail("no -cacheArchive option any more, please use -resources instead.");
    }

    String[] caf = cmdLine.getOptionValues("cacheFile");
    if (null != caf) {
      fail("no -cacheFile option any more, please use -resources instead.");
    }

    mapCmd_ = cmdLine.getOptionValue("mapper");

    String[] cmd = cmdLine.getOptionValues("cmdenv");
    if (null != cmd && cmd.length > 0) {
      for (String s : cmd) {
        if (addTaskEnvironment_.length() > 0) {
          addTaskEnvironment_ += " ";
        }
        addTaskEnvironment_ += s;
      }
    }

    // per table input config
    Map<String, Map<String, String>> inputConfigs =
        new HashMap<String, Map<String, String>>();
    String[] columns = null;

    for (Option opt : cmdLine.getOptions()) {
      if ("jobconf".equals(opt.getOpt())) {
        String[] jobconf = opt.getValues();
        if (null != jobconf && jobconf.length > 0) {
          for (String s : jobconf) {
            String[] parts = s.split("=", 2);
            config_.set(parts[0], parts[1]);
          }
        }
      } else if ("columns".equals(opt.getOpt())) {
        String columnsValue = opt.getValue();
        if (columnsValue.equals("ALL")) {
          columns = null;
        } else {
          columns = columnsValue.split(",");
        }
      } else if ("input".equals(opt.getOpt())) {
        values = opt.getValues();
        if (values != null && values.length > 0) {
          for (String input : values) {
            TableInfo ti = parseTableInfo(input);
            if (columns != null) {
              ti.setCols(columns);
            }
            inputSpecs_.add(ti);

            String inputKey = (ti.getProjectName() + "." + ti.getTableName()).toLowerCase();
            // XXX only apply once per table
            if (inputConfigs.get(inputKey) != null) {
              continue;
            }

            Map<String, String> inputConfig = new HashMap<String, String>();
            inputConfig.put("stream.map.input.field.separator",
                            config_.get("stream.map.input.field.separator", "\t"));
            // TODO other per table input config: cols, etc.
            inputConfigs.put(inputKey, inputConfig);
          }
        }
      }
    }
    try {
      config_.set("stream.map.input.configs",
                  JSON.toJSONString(inputConfigs));
    } catch (Exception e) {
      throw new RuntimeException("fail to set input configs");
    }
  }

  protected void msg(String msg) {
    if (verbose_) {
      System.out.println("STREAM: " + msg);
    }
  }

  private Option createOption(String name, String desc,
                              String argName, int max, boolean required) {
    return OptionBuilder
        .withArgName(argName)
        .hasArgs(max)
        .withDescription(desc)
        .isRequired(required)
        .create(name);
  }

  private Option createBoolOption(String name, String desc) {
    return OptionBuilder.withDescription(desc).create(name);
  }

  private void setupOptions() {

    // input and output are not required for -info and -help options,
    // though they are required for streaming job to be run.
    Option input = createOption("input",
                                "Input tables/partitions for the Map step",
                                "path",
                                Integer.MAX_VALUE,
                                false);

    Option columns = createOption("columns",
                                  "Input table column names for the Map step",
                                  "spec",
                                  1,
                                  false);

    Option output = createOption("output",
                                 "Result table/partition for the Reduce step",
                                 "path", 1, false);
    Option mapper = createOption("mapper",
                                 "The streaming command to run", "cmd", 1, false);
    Option combiner = createOption("combiner",
                                   "The streaming command to run", "cmd", 1, false);
    // reducer could be NONE
    Option reducer = createOption("reducer",
                                  "The streaming command to run", "cmd", 1, false);
    Option file = createOption("file",
                               "File to be shipped in the Job jar file",
                               "file", Integer.MAX_VALUE, false);
    Option additionalconfspec = createOption("additionalconfspec",
                                             "Optional.", "spec", 1, false);
    Option partitioner = createOption("partitioner",
                                      "Optional.", "spec", 1, false);
    Option numReduceTasks = createOption("numReduceTasks",
                                         "Optional.", "spec", 1, false);
    Option mapDebug = createOption("mapdebug",
                                   "Optional.", "spec", 1, false);
    Option reduceDebug = createOption("reducedebug",
                                      "Optional", "spec", 1, false);
    Option jobconf =
        createOption("jobconf",
                     "(n=v) Optional. Add or override a JobConf property.",
                     "spec", 1, false);

    Option cmdenv =
        createOption("cmdenv", "(n=v) Pass env.var to streaming commands.",
                     "spec", 1, false);
    Option cacheFile = createOption("cacheFile",
                                    "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
    Option cacheArchive = createOption("cacheArchive",
                                       "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
    Option io = createOption("io",
                             "Optional.", "spec", 1, false);

    // boolean properties

    Option
        background =
        createBoolOption("background", "Submit the job and don't wait till it completes.");
    Option verbose = createBoolOption("verbose", "print verbose output");
    Option info = createBoolOption("info", "print verbose output");
    Option help = createBoolOption("help", "print this help message");
    Option debug = createBoolOption("debug", "print debug output");
    Option lazyOutput = createBoolOption("lazyOutput", "create outputs lazily");

    allOptions = new Options().
        addOption(input).
        addOption(columns).
        addOption(output).
        addOption(mapper).
        addOption(combiner).
        addOption(reducer).
        addOption(file).
        addOption(additionalconfspec).
        addOption(partitioner).
        addOption(numReduceTasks).
        addOption(mapDebug).
        addOption(reduceDebug).
        addOption(jobconf).
        addOption(cmdenv).
        addOption(cacheFile).
        addOption(cacheArchive).
        addOption(io).
        addOption(background).
        addOption(verbose).
        addOption(info).
        addOption(debug).
        addOption(help).
        addOption(lazyOutput);
  }

  public void exitUsage(boolean detailed) {
    printUsage(detailed);
    fail("");
  }

  private void printUsage(boolean detailed) {
    System.out.println(
        "Usage: jar [-classpath ...] [-resources ...] com.aliyun.odps.mapred.bridge.streaming.StreamJob"
        + " [options]");
    System.out.println("Options:");
    System.out
        .println("  -input          <[/prj/]tbl/[pt=x[/ds=y]]> input table/partition for the Map"
                 + " step.");
    System.out.println("  -output         <[/prj/]tbl/[pt=x[/ds=y]]> output table/partition for the"
                       + " Reduce step.");
    System.out.println("  -mapper         <cmd|JavaClassName> Optional. Command"
                       + " to be run as mapper.");
    System.out.println("  -combiner       <cmd|JavaClassName> Optional. Command"
                       + " to be run as combiner.");
    System.out.println("  -reducer        <cmd|JavaClassName> Optional. Command"
                       + " to be run as reducer.");
    System.out.println("  -file           <file> Optional. Local file/dir to be "
                       + "shipped with the streaming job.");
    System.out.println("  -partitioner    <JavaClassName>  Optional. The"
                       + " partitioner class.");
    System.out.println("  -numReduceTasks <num> Optional. Number of reduce "
                       + "tasks.");
    System.out.println("  -cmdenv         <n>=<v> Optional. Pass env.var to"
                       + " streaming commands.");
    System.out.println("  -lazyOutput     Optional. Lazily create Output.");
    System.out
        .println("  -background     Optional. Submit the job and don't wait till it completes.");
    System.out.println("  -verbose        Optional. Print verbose output.");
    System.out.println("  -info           Optional. Print detailed usage.");
    System.out.println("  -help           Optional. Print help message.");
    System.out.println();

    if (!detailed) {
      System.out.println();
      System.out.println("For more details about these options use -info option.");
      return;
    }
    System.out.println();
    System.out.println("Usage tips:");
    System.out.println("To set the number of reduce tasks (num. of output " +
                       "files) as, say 10:");
    System.out.println("  Use -numReduceTasks 10");
    System.out.println("To skip the sort/combine/shuffle/sort/reduce step:");
    System.out.println("  Use -numReduceTasks 0");
    System.out.println("  Map output then becomes a 'side-effect " +
                       "output' rather than a reduce input.");
    System.out.println("  This speeds up processing. This also feels " +
                       "more like \"in-place\" processing");
    System.out.println("  because the input filename and the map " +
                       "input order are preserved.");
    System.out.println("  This is equivalent to -reducer NONE");
    System.out.println();
    System.out.println("To treat tasks with non-zero exit status as SUCCEDED:");
    System.out.println("  -D stream.non.zero.exit.is.failure=false");
    System.out.println("To set an environement variable in a streaming " +
                       "command:");
    System.out.println("   -cmdenv EXAMPLE_DIR=/home/example/dictionaries/");
  }

  public void fail(String message) {
    System.err.println(message);
    System.err.println("Try -help for more information");
    throw new IllegalArgumentException(message);
  }

  // --------------------------------------------

  /**
   * Parse table input/output path to TableInfo.
   * Supported pattern:
   * /prj/tbl
   * /prj/tbl/pt=x/ds=y
   * tbl
   * tbl/pt=x/ds=y
   */
  private static TableInfo parseTableInfo(String tableInfoStr) {
    String prj = SessionState.get().getOdps().getDefaultProject();
    if (prj == null) {
      // should not happen
      throw new RuntimeException("default project should have been set");
    }
    String tbl = null;
    String part = null;
    if (tableInfoStr.startsWith("/")) {
      String[] parts = tableInfoStr.substring(1).split("/", 3);
      if (parts.length < 2) {
        throw new IllegalArgumentException("invalid table info: " + tableInfoStr);
      }
      prj = parts[0];
      tbl = parts[1];
      if (parts.length == 3) {
        part = parts[2];
      }
    } else {
      String[] parts = tableInfoStr.split("/", 2);
      if (parts.length == 0) {
        throw new IllegalArgumentException("invalid table info: " + tableInfoStr);
      }
      tbl = parts[0];
      if (parts.length == 2) {
        part = parts[1];
      }
    }

    TableInfo.TableInfoBuilder builder = TableInfo.builder();
    builder.projectName(prj);
    builder.tableName(tbl);
    if (part != null) {
      builder.partSpec(part);
    }
    return builder.build();
  }

  protected void setJobConf() throws IOException {

    // general MapRed job properties
    jobConf_ = new JobConf(config_);

    // All streaming jobs get the task timeout value
    // from the configuration settings.

    for (int i = 0; i < inputSpecs_.size(); i++) {
      InputUtils.addTable(inputSpecs_.get(i), jobConf_);
    }

    String defaultPackage = this.getClass().getPackage().getName();

    if (ioSpec_ != null) {
      jobConf_.set("stream.map.input", ioSpec_);
      jobConf_.set("stream.map.output", ioSpec_);
      jobConf_.set("stream.reduce.input", ioSpec_);
      jobConf_.set("stream.reduce.output", ioSpec_);
    }

    //Class<? extends IdentifierResolver> idResolverClass =
    //  jobConf_.getClass("stream.io.identifier.resolver.class",
    //    IdentifierResolver.class, IdentifierResolver.class);
    //IdentifierResolver idResolver = ReflectionUtils.newInstance(idResolverClass, jobConf_);

    //idResolver.resolve(jobConf_.get("stream.map.input", IdentifierResolver.TEXT_ID));
    //jobConf_.setClass("stream.map.input.writer.class",
    //  idResolver.getInputWriterClass(), InputWriter.class);
    jobConf_.setClass("stream.map.input.writer.class", RecordInputWriter.class, InputWriter.class);

    //idResolver.resolve(jobConf_.get("stream.reduce.input", IdentifierResolver.TEXT_ID));
    //jobConf_.setClass("stream.reduce.input.writer.class",
    //  idResolver.getInputWriterClass(), InputWriter.class);
    jobConf_.setClass("stream.reduce.input.writer.class", TextInputWriter.class, InputWriter.class);

    jobConf_.set("stream.addenvironment", addTaskEnvironment_);

    boolean isMapperACommand = false;
    Class c = null;
    if (mapCmd_ != null) {
      c = StreamUtil.goodClassOrNull(jobConf_, mapCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setMapperClass(c);
      } else {
        isMapperACommand = true;
        jobConf_.setMapperClass(PipeMapper.class);
        //jobConf_.setMapRunnerClass(PipeMapRunner.class);
        jobConf_.set("stream.map.streamprocessor",
                     URLEncoder.encode(mapCmd_, "UTF-8"));
      }
    }

    if (comCmd_ != null) {
      c = StreamUtil.goodClassOrNull(jobConf_, comCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setCombinerClass(c);
      } else {
        jobConf_.setCombinerClass(PipeCombiner.class);
        jobConf_.set("stream.combine.streamprocessor", URLEncoder.encode(
            comCmd_, "UTF-8"));
      }
    }

    if (numReduceTasksSpec_ != null) {
      int numReduceTasks = Integer.parseInt(numReduceTasksSpec_);
      jobConf_.setNumReduceTasks(numReduceTasks);
    }

    boolean isReducerACommand = false;
    if (redCmd_ != null) {
      if (redCmd_.equals(REDUCE_NONE)) {
        jobConf_.setNumReduceTasks(0);
      }
      if (jobConf_.getNumReduceTasks() != 0) {
        if (redCmd_.compareToIgnoreCase("aggregate") == 0) {
          //jobConf_.setReducerClass(ValueAggregatorReducer.class);
          //jobConf_.setCombinerClass(ValueAggregatorCombiner.class);
          // TODO reducer lib
          throw new UnsupportedOperationException("'aggregate' reducer not supported yet");
        } else {
          c = StreamUtil.goodClassOrNull(jobConf_, redCmd_, defaultPackage);
          if (c != null) {
            jobConf_.setReducerClass(c);
          } else {
            isReducerACommand = true;
            jobConf_.setReducerClass(PipeReducer.class);
            jobConf_.set("stream.reduce.streamprocessor", URLEncoder.encode(
                redCmd_, "UTF-8"));
          }
        }
      }
    }

    String
        mapOutputFieldSeparator =
        unescapeSeparator(jobConf_.get("stream.map.output.field.separator", "\t"));
    String
        reduceInputFieldSeparator =
        unescapeSeparator(jobConf_.get("stream.reduce.input.field.separator", "\t"));
    int numOfMapOutputKeyFields = jobConf_.getInt("stream.num.map.output.key.fields", 1);

    if (numOfMapOutputKeyFields > 1 && !mapOutputFieldSeparator.equals(reduceInputFieldSeparator)) {
      throw new IllegalArgumentException(
          "for multiple-fields key, stream.reduce.input.field.separator should be the same as stream.map.output.field.separator to avoid confusion");
    }

    Column[] mapOutputKeySchema = new Column[numOfMapOutputKeyFields];

    Map<Integer, KeyDescription> keyOptions =
        parseKeyOptions(jobConf_.get("stream.map.output.key.options", ""));

    for (int i = 0; i < mapOutputKeySchema.length; i++) {
      KeyDescription keyDesc = keyOptions.get(i + 1);
      OdpsType t = (keyDesc == null || !keyDesc.numeric) ? OdpsType.STRING : OdpsType.BIGINT;
      mapOutputKeySchema[i] = new Column("map_out_key" + i, t);
    }
    jobConf_.setMapOutputKeySchema(mapOutputKeySchema);

    if (!keyOptions.isEmpty()) {
      JobConf.SortOrder[] sortOrder = new JobConf.SortOrder[mapOutputKeySchema.length];
      for (int i = 0; i < mapOutputKeySchema.length; i++) {
        KeyDescription keyDesc = keyOptions.get(i + 1);
        sortOrder[i] =
            (keyDesc == null || !keyDesc.reverse) ? JobConf.SortOrder.ASC : JobConf.SortOrder.DESC;
      }
      jobConf_.setOutputKeySortOrder(sortOrder);
    }

    jobConf_.setMapOutputValueSchema(new Column[]{new Column("map_out_value", OdpsType.STRING)});

    // use setPartitionColumns for KeyFieldBasedPartitioner
    if (partitionerSpec_ != null) {
      if (partitionerSpec_.equals("KeyFieldBasedPartitioner")) {
        partitionerSpec_ = "com.aliyun.odps.mapred.lib.KeyFieldBasedPartitioner";
      }
      if (partitionerSpec_.equals("com.aliyun.odps.mapred.lib.KeyFieldBasedPartitioner")) {
        String
            mapOutputKeyFieldSeparator =
            unescapeSeparator(jobConf_.get("map.output.key.field.separator", "\t"));
        if (mapOutputFieldSeparator.equals(mapOutputKeyFieldSeparator)) {
          int numOfKeyFieldsForPartition = jobConf_.getInt("num.key.fields.for.partition", 1);
          if (numOfKeyFieldsForPartition > numOfMapOutputKeyFields) {
            throw new IllegalArgumentException(
                "num.key.fields.for.partition should not bigger than stream.num.map.output.key.fields");
          }
          if (numOfKeyFieldsForPartition < numOfMapOutputKeyFields) {
            String[] partitionColumns = new String[numOfKeyFieldsForPartition];
            for (int i = 0; i < numOfKeyFieldsForPartition; i++) {
              partitionColumns[i] = mapOutputKeySchema[i].getName();
            }
            jobConf_.setPartitionColumns(partitionColumns);
          }
        } else {
          // need to split the first field for partition, only for compatible with hadoop.
          // FIXME this partitioner would be implemented by the StreamingOperator at runtime...
          c = StreamUtil.goodClassOrNull(jobConf_, partitionerSpec_, defaultPackage);
          if (c != null) {
            jobConf_.setPartitionerClass(c);
          }
        }
      } else {
        throw new IllegalArgumentException(
            "User defined partitioner not supported for streaming job");
      }
    }

    Class mapOutputReaderClass = TextOutputReader.class;
    Class reduceOutputReaderClass = RecordOutputReader.class;
    if (jobConf_.getNumReduceTasks() > 0) {
      boolean hasKey = jobConf_.getInt("stream.num.reduce.output.key.fields", 0) > 0;
      reduceOutputReaderClass = hasKey ? TextOutputReader.class : RecordOutputReader.class;
    } else {
      boolean hasKey = jobConf_.getInt("stream.num.map.output.key.fields", 0) > 0;
      mapOutputReaderClass = hasKey ? TextOutputReader.class : RecordOutputReader.class;
    }
    jobConf_.setClass("stream.map.output.reader.class", mapOutputReaderClass, OutputReader.class);
    jobConf_
        .setClass("stream.reduce.output.reader.class", reduceOutputReaderClass, OutputReader.class);

    // XXX no-output allowed
    if (output_ != null) {
      OutputUtils.addTable(parseTableInfo(output_), jobConf_);
    }

    //if(mapDebugSpec_ != null){
    //	jobConf_.setMapDebugScript(mapDebugSpec_);
    //}
    //if(reduceDebugSpec_ != null){
    //	jobConf_.setReduceDebugScript(reduceDebugSpec_);
    //}
    // last, allow user to override anything
    // (although typically used with properties we didn't touch)

    // FIXME resources linkname

    if (verbose_) {
      listJobConfProperties();
    }
  }

  /**
   * Prints out the jobconf properties on stdout
   * when verbose is specified.
   */
  protected void listJobConfProperties() {
    msg("==== JobConf properties:");
    TreeMap<String, String> sorted = new TreeMap<String, String>();
    for (final Map.Entry<String, String> en : jobConf_) {
      sorted.put(en.getKey(), en.getValue());
    }
    for (final Map.Entry<String, String> en : sorted.entrySet()) {
      msg(en.getKey() + "=" + en.getValue());
    }
    msg("====");
  }

  // Based on JobClient
  public int submitAndMonitorJob() throws Exception {
    running_ = JobClient.submitJob(jobConf_);
    LOG.debug("submit job done");
    if (background_) {
      LOG.info("Job is running in background.");
    } else {
      running_.waitForCompletion();
      if (!running_.isSuccessful()) {
        return 1;
      }
    }
    return 0;
  }

  private static class KeyDescription {

    boolean numeric = false;
    boolean reverse = false;
  }

  ;

  /**
   * Parse key option;
   * option is like f[n][r],f[n][r],...
   */
  private Map<Integer, KeyDescription> parseKeyOptions(String options) {
    Map<Integer, KeyDescription> keys = new HashMap<Integer, KeyDescription>();

    StringTokenizer st = new StringTokenizer(options, "nr,", true);
    while (st.hasMoreTokens()) {
      String token = st.nextToken();
      int fieldId;
      try {
        fieldId = Integer.parseInt(token);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "invalid key options format, expect field number at '" + token + "'");
      }
      KeyDescription keyDesc = new KeyDescription();

      while (st.hasMoreTokens()) {
        token = st.nextToken();
        if (token.equals(",")) {
          break;
        } else if (token.equals("n")) {
          keyDesc.numeric = true;
        } else if (token.equals("r")) {
          keyDesc.reverse = true;
        } else {
          throw new IllegalArgumentException(
              "invalid key options format, unknown option '" + token + "'");
        }
      }
      keys.put(fieldId, keyDesc);
    }
    return keys;
  }

  protected String[] argv_;
  protected boolean background_;
  protected boolean verbose_;
  protected boolean detailedUsage_;
  protected boolean printUsage = false;
  protected int debug_;

  protected Environment env_;

  protected JobConf config_;
  protected JobConf jobConf_;

  // command-line arguments
  protected ArrayList<TableInfo> inputSpecs_ = new ArrayList<TableInfo>();
  protected ArrayList<String> packageFiles_ = new ArrayList<String>();
  protected ArrayList<String> shippedCanonFiles_ = new ArrayList<String>();
  //protected TreeMap<String, String> userJobConfProps_ = new TreeMap<String, String>();
  protected String output_;
  protected String mapCmd_;
  protected String comCmd_;
  protected String redCmd_;
  protected String partitionerSpec_;
  protected String numReduceTasksSpec_;
  protected String additionalConfSpec_;
  protected String mapDebugSpec_;
  protected String reduceDebugSpec_;
  protected String ioSpec_;
  protected boolean lazyOutput_;

  // Use to communicate config to the external processes (ex env.var.HADOOP_USER)
  // encoding "a=b c=d"
  protected String addTaskEnvironment_;

  protected RunningJob running_;
  protected static final String
      LINK_URI =
      "You need to specify the uris as scheme://path#linkname," +
      "Please specify a different link name for all of your caching URIs";


  public static void main(String[] args) {
    StreamJob job = new StreamJob();
    try {
      System.exit(job.run(args));
    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit(1);
    }
  }
}
