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

package com.aliyun.odps.udf.local;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.local.datasource.InputSource;
import com.aliyun.odps.udf.local.datasource.TableInputSource;
import com.aliyun.odps.udf.local.runner.BaseRunner;
import com.aliyun.odps.udf.local.runner.RunnerFactory;

import static com.aliyun.odps.local.common.utils.LocalRunUtils.validateTunnelEndpoint;

public class Main {

  private Options options;

  @SuppressWarnings("static-access")
  public Main() {
    options = new Options();

    options.addOption("c", "classname", true, "UDF,UDTF or UDAF class name");
    options.addOption("i", "input", true,
                      "input table: table.p(p1=1,p2=2).c(c1,c2)");

    options.addOption("p", "project", true, "Default project name");
    options.addOption(null, "endpoint", true, "Specify the endpoint");
    options.addOption(null, "access-id", true, "The accessId of ALIYUN account");
    options.addOption(null, "access-key", true, "The accessKey of ALIYUN account");
    options.addOption(null, "tunnel-endpoint", true, "Specify the customer tunnel endpoint");
    options.addOption("w", "warehouse-dir", true, "Warehouse Dir");
    options.addOption(null, "record-limit", true, "download record num limit");
    options.addOption(null, "column-separator", true, "column separator for data input");
    options.addOption("h", "help", false, "print this help information");
  }

  public static void main(String[] args) throws ParseException, LocalRunException, UDFException,
                                                IOException {
    Main driver = new Main();
    driver.run(args);
  }

  public void run(String[] args) throws LocalRunException {

    try {
      PosixParser parser = new PosixParser();
      CommandLine cmdl = parser.parse(options, args);
      validArgments(cmdl);

      String defaultProject = cmdl.getOptionValue("project");
      String endpoint = cmdl.getOptionValue("endpoint");
      String accessId = cmdl.getOptionValue("access-id");
      String accessKey = cmdl.getOptionValue("access-key");

      Account account = new AliyunAccount(accessId, accessKey);
      Odps odps = new Odps(account);
      odps.setEndpoint(endpoint);
      odps.setDefaultProject(defaultProject);

      String warehouseDir = cmdl.getOptionValue("w");
      if (StringUtils.isNotBlank(warehouseDir)) {
        WareHouse.getInstance(warehouseDir); // init warehouse with giving dir
      }

      setTunnelEndpoint(cmdl.getOptionValue("tunnel-endpoint"));
      WareHouse.getInstance().setRecordLimit(cmdl.getOptionValue("record-limit"));
      WareHouse.getInstance().setColumnSeparator(cmdl.getOptionValue("column-separator"));

      BaseRunner runner = RunnerFactory.getRunner(cmdl, odps);

      InputSource inputSource = getInputSource(cmdl);

      Object[] data;
      while ((data = inputSource.getNextRow()) != null) {
        runner.feed(data);
      }

      List<Object[]> result = runner.yield();
      for (Object[] record : result) {
        System.out.println(StringUtils.join(record, ","));
      }
    } catch (ParseException e) {
      throw new LocalRunException(e);
    } catch (LocalRunException e) {
      throw e;
    } catch (UDFException e) {
      throw new LocalRunException(e);
    } catch (IOException e) {
      throw new LocalRunException(e);
    }

  }

  public Options getOptions() {
    return options;
  }

  public void setOptions(Options options) {
    this.options = options;
  }

  private void validArgments(CommandLine cmdl) {
    if (cmdl.hasOption("h")) {
      printHelpAndExit(0);
    }

    if (!cmdl.hasOption("c")) {
      System.err.println("missing argument '-c'");
      printHelpAndExit(1);
    }

    if (!cmdl.hasOption("i")) {
      System.err.println("missing argument '-i'");
      printHelpAndExit(1);
    }
  }

  private InputSource getInputSource(CommandLine cmdl) throws LocalRunException {
    String inputArgs = cmdl.getOptionValue("i");
    // ie: table.p(p1=1,p2=2).c(c1,c2)
    String[] ss = inputArgs.split("\\.");
    if (ss == null || ss.length == 0) {
      throw new LocalRunError("Input table can't null ");
    }
    String table = ss[0].trim();
    String[] partitions = null;
    String[] columns = null;
    for (int i = 1; i < ss.length; i++) {
      String str = ss[i].trim();
      if (str.startsWith("p(")) {
        if (str.length() < 6) {
          throw new LocalRunError("Invalid partitions ");
        }
        str = str.substring(2, str.length() - 1);
        partitions = str.split(",");
        for (int j = 0; j < partitions.length; j++) {
          if (partitions[j].split("=").length != 2) {
            throw new LocalRunError("Invalid partitions:" + str);
          }
        }
      } else if (str.startsWith("c(")) {
        if (str.length() < 4) {
          throw new LocalRunError("Invalid columns");
        }
        str = str.substring(2, str.length() - 1);
        columns = str.split(",");
      } else {
        throw new LocalRunError("Invalid input table info, please check it");
      }
    }

    return new TableInputSource(WareHouse.getInstance().getOdps().getDefaultProject(), table,
                                partitions, columns);
  }

  private void printHelpAndExit(int exitCode) {
    HelpFormatter help = new HelpFormatter();
    help.printHelp("judt", options);
    System.exit(exitCode);
  }

  public void setTunnelEndpoint(String tunnelEndpoint) throws IllegalArgumentException{
    if (StringUtils.isBlank(tunnelEndpoint)) {
      return;
    }
    if (validateTunnelEndpoint(tunnelEndpoint)) {
      WareHouse.getInstance().setTunnelEndpoint(tunnelEndpoint);
    } else {
      throw new IllegalArgumentException("Invalid tunnel endpoint: " + tunnelEndpoint);
    }
  }

}
