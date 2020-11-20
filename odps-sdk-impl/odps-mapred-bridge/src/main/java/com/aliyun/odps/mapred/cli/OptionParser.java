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

package com.aliyun.odps.mapred.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import com.aliyun.odps.account.StsAccount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mapred.conf.SessionState;


/**
 * Cli argument parser.
 */
@SuppressWarnings("static-access")
public class OptionParser {

  private static Options options = new Options();

  static {
    // Init cli options
    Option resources = OptionBuilder.withArgName("resources").hasArg()
        .withDescription("Library resource names, seperated by comma").create("resources");
    options.addOption(resources);
    Option jobconf = OptionBuilder.withArgName("jobconf").hasArg()
        .withDescription("Job config parameters, overriding default mapreduce-conf.xml")
        .create("jobconf");
    options.addOption(jobconf);
    Option odpsconf = OptionBuilder.withArgName("odpsconf").hasArg()
        .withDescription("Odps config parameters, overriding default odps-conf.xml")
        .create("odpsconf");
    options.addOption(odpsconf);
    Option job = OptionBuilder.withArgName("job").hasArg()
        .withDescription("Job config file path, overriding default mapreduce-conf.xml")
        .create("job");
    options.addOption(job);
    Option odps = OptionBuilder.withArgName("odps").hasArg()
        .withDescription("Odps config file path, overriding default odps-conf.xml").create("odps");
    options.addOption(odps);

    Option aliases = OptionBuilder.withArgName("aliases").hasArg()
        .withDescription("Odps alias settings").create("aliases");
    options.addOption(aliases);

    Option localrun = OptionBuilder.withArgName("local").withDescription("Enable local run mode.")
        .create("local");
    options.addOption(localrun);
  }

  private Class<?> mainClass;
  private String[] arguments;
  private SessionState ss;

  public OptionParser(SessionState ss) {
    this.ss = ss;
  }

  public void parse(String[] args) throws ParseException, FileNotFoundException,
                                          ClassNotFoundException, OdpsException {
    CommandLineParser parser = new BasicParser();

    if (args == null || args.length <= 0) {
      throw new UnsupportedOperationException("Main class not specified.");
    }

    CommandLine cmd = parser.parse(options, args, true);

    // Config load order: default file -> specified by file names -> specified
    // by kv pairs.
    OdpsConf odpsConf = new OdpsConf();
    if (cmd.hasOption("odps")) {
      String odpsconf = cmd.getOptionValue("odps");
      File conf = new File(odpsconf);
      InputStream in = new FileInputStream(conf);
      odpsConf.addResource(in);
    }

    if (cmd.hasOption("odpsconf")) {
      String[] odpsconf = cmd.getOptionValues("odpsconf");
      for (String conf : odpsconf) {
        String[] kv = conf.split("=", 2);
        odpsConf.set(kv[0], kv[1]);
      }
    }

    Account account = getAccount(odpsConf);
    Odps odps = new Odps(account);
    odps.setDefaultProject(odpsConf.getProjName());
    if (odpsConf.getEndpoint() != null) {
      odps.setEndpoint(odpsConf.getEndpoint());
    }
    ss.setOdps(odps);

    if (cmd.hasOption("job")) {
      String jobconf = cmd.getOptionValue("job");
      File conf = new File(jobconf);
      InputStream in = new FileInputStream(conf);
      ss.getDefaultJob().addResource(in);
    }

    if (cmd.hasOption("jobconf")) {
      String[] jobconfs = cmd.getOptionValues("jobconf");
      for (String conf : jobconfs) {
        String[] kv = conf.split("=", 2);
        ss.getDefaultJob().set(kv[0], kv[1]);
      }
    }

    if (cmd.hasOption("aliases")) {
      String aliases = cmd.getOptionValue("aliases");
      Map<String, String> map;
      try {
        map = new GsonBuilder().disableHtmlEscaping().create()
                .fromJson(aliases, new TypeToken<Map<String, String>>() {}.getType());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ss.setAliases(map);
    }

    if (cmd.hasOption("resources")) {
      String resources = cmd.getOptionValue("resources");
      ss.getDefaultJob().setResources(resources);
    }

    if (cmd.hasOption("local")) {
      ss.setLocalRun(true);
    }

    String[] remain = cmd.getArgs();
    if (remain == null || remain.length <= 0) {
      throw new UnsupportedOperationException("Main class not specified.");
    }
    mainClass = Class.forName(remain[0]);
    if (remain.length > 1) {
      arguments = Arrays.copyOfRange(remain, 1, remain.length);
    } else {
      arguments = new String[0];
    }

  }

  public void usage() {
    new HelpFormatter().printHelp("mapreduce [<GENERIC_OPTIONS>] <MAIN_CLASS> [ARGS]", options);
  }

  public Class<?> getMainClass() {
    return mainClass;
  }

  public String[] getArguments() {
    return arguments;
  }

  private Account getAccount(OdpsConf odpsConf) throws OdpsException {
    AccountProvider accountProvider = odpsConf.getAccountProvider();

    switch (accountProvider) {
      case ALIYUN:
        return new AliyunAccount(odpsConf.getAccessId(), odpsConf.getAccessKey());


      case STS:
        return new StsAccount(odpsConf.getAccessId(),
                              odpsConf.getAccessKey(),
                              odpsConf.getStsToken());
      default:
        throw new OdpsException("unsupport account provider:" + accountProvider);
    }

  }

}
