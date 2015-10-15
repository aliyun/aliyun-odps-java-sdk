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

package com.aliyun.odps.udf.local.runner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;

import com.aliyun.odps.Odps;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.security.ApplicatitionType;
import com.aliyun.odps.local.common.security.SecurityClient;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.local.LocalExecutionContext;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.datasource.InputSource;

public abstract class BaseRunner {
  boolean hasClosed = false;

  public BaseRunner(Odps odps) {
    WareHouse.getInstance().setOdps(odps);
    initSecurity();
  }

  private void initSecurity() {
    List<String> codeBase = new LinkedList<String>();
    // add odps-udf-local
    String path = BaseRunner.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    path = path.substring(path.indexOf(":") + 1);
    codeBase.add(path);

    // add odps-sdk-udf
    path = UDF.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    path = path.substring(path.indexOf(":") + 1);
    codeBase.add(path);

    // add odps-common-local
    path = WareHouse.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    path = path.substring(path.indexOf(":") + 1);
    codeBase.add(path);

    boolean isSecurityEnabled =
        System.getProperty(Constants.LOCAL_SECURITY_ENABLE, "false").trim()
            .equalsIgnoreCase("true");
    boolean isJNIEnabled =
        System.getProperty(Constants.LOCAL_SECURITY_JNI_ENABLE, "false").trim()
            .equalsIgnoreCase("true");
    String userDefinePolicy = System.getProperty(Constants.LOCAL_USER_DEFINE_POLICY, "").trim();
    SecurityClient.init(ApplicatitionType.UDF, codeBase, null, isSecurityEnabled, isJNIEnabled,
        userDefinePolicy);
  }

  protected LocalExecutionContext context = new LocalExecutionContext();
  protected List<Object[]> buffer = new ArrayList<Object[]>();
  protected List<InputSource> inputSources = new LinkedList<InputSource>();

  
  /**
   * 添加输入源，可添加多个，框架处理数据输入的顺序为： <br/>
   * 先处理通过feed和feedAll加入的数据（两种按先后顺序）， 
   * 最后处理通过addInputSource添加的数据源中的数据（多个InputSource按添加先后顺序处理）
   *
   * 代码示例：
   * 
   * <pre>
   * BaseRunner runner = new UDFRunner(odps, new UdfExample());
   * 
   * // partition table
   * String project = &quot;project_name&quot;;
   * String table = &quot;wc_in2&quot;;
   * String[] partitions = null;
   * String[] columns = new String[] {&quot;colc&quot;, &quot;cola&quot;};
   * partitions = new String[] {&quot;p2=1&quot;, &quot;p1=2&quot;};
   * 
   * // input1
   * InputSource inputSource = new TableInputSource(project, table, partitions, columns);
   * runner.addInputSource(inputSource);
   * 
   * // input2
   * Object[][] inputs1 = new Object[2][];
   * inputs1[0] = new Object[] {&quot;one&quot;, &quot;one&quot;};
   * inputs1[1] = new Object[] {&quot;two&quot;, &quot;two&quot;};
   * runner.feedAll(inputs1);
   * 
   * List&lt;Object[]&gt; out = runner.yield();
   * Assert.assertEquals(5, out.size());
   * Assert.assertEquals(&quot;ss2s:one,one&quot;, StringUtils.join(out.get(0), &quot;,&quot;));
   * Assert.assertEquals(&quot;ss2s:two,two&quot;, StringUtils.join(out.get(1), &quot;,&quot;));
   * Assert.assertEquals(&quot;ss2s:three3,three1&quot;, StringUtils.join(out.get(2), &quot;,&quot;));
   * Assert.assertEquals(&quot;ss2s:three3,three1&quot;, StringUtils.join(out.get(3), &quot;,&quot;));
   * Assert.assertEquals(&quot;ss2s:three3,three1&quot;, StringUtils.join(out.get(4), &quot;,&quot;));
   * </pre>
   *
   * @param inputSource
   * @return
   */
  public void addInputSource(InputSource inputSource) {
    if (inputSource != null) {
      try {
        inputSource.setup();
      } catch (IOException e) {
        close();
        throw new RuntimeException(e);
      } 
      inputSources.add(inputSource);
    }
  }

  /**
   * case的输入数据，每次调用只传递一组输入数据
   *
   * 代码示例：
   * <pre>
   *  BaseRunner runner = new UDFRunner(odps, new UdfExample());
   *  runner.feed(new Object[] { "one", "one" })
   *        .feed(new Object[] { "three", "three" })
   *        .feed(new Object[] { "four", "four" });
   *  </pre>
   *
   * @param input
   * @return
   * @throws LocalRunException
   */
  public BaseRunner feed(Object[] input) throws LocalRunException {
    try {
      return internalFeed(input);
    } catch (LocalRunException e) {
      close();
      throw e;
    }
  };
  
  protected abstract BaseRunner internalFeed(Object[] input) throws LocalRunException;

  /**
   * case的输入数据，每次调用传递多组输入数据
   *
   * 代码示例：
   * <pre>
   *  BaseRunner runner = new UDFRunner(odps, new UdfExample());
   *  Object[][] inputs = new Object[3][];
   *  inputs[0] = new Object[] { "one", "one" };
   *  inputs[1] = new Object[] { "three", "three" };
   *  inputs[2] = new Object[] { "four", "four" };
   *  runner.feedAll(inputs);
   *  </pre>
   *
   * @param inputs
   * @return
   * @throws LocalRunException
   */
  public BaseRunner feedAll(Object[][] inputs) throws LocalRunException {
    if (inputs == null) {
      return this;
    }
    for (Object[] input : inputs) {
      feed(input);
    }
    return this;
  }

  /**
   * case的输入数据，每次调用传递多组输入数据
   *
   * 代码示例：
   * <pre>
   *  BaseRunner runner = new UDFRunner(odps, new UdfExample());
   *  List<Object[]> inputs = new ArrayList<Object[]>();
   *  inputs.add(new Object[] { "one", "one" });
   *  inputs.add(new Object[] { "three", "three" });
   *  inputs.add(new Object[] { "four", "four" });
   *  runner.feedAll(inputs);
   *  </pre>
   *
   * @param inputs
   * @return
   * @throws LocalRunException
   */
  public BaseRunner feedAll(List<Object[]> inputs) throws LocalRunException {
    if (inputs == null) {
      return this;
    }
    for (Object[] input : inputs) {
      feed(input);
    }
    return this;
  }

  /**
   * 根据case输入数据产生输出结果，
   * 调用此方法后将不能再次调用feed及feedAll,
   * 如果需要添加更多case需要重新构造runner,
   * 用户可以对输出结果进行校验
   *
   * <pre>
   * BaseRunner runner = new UDFRunner(odps, new UdfExample());
   * List<Object[]> out =runner.feed(new Object[] { "one", "one" })
   * .feed(new Object[] {"three", "three" })
   * .feed(new Object[] { "four", "four" })
   * .yield();
   *
   * Assert.assertEquals(3, out.size());
   * Assert.assertEquals("ss2s:one,one", StringUtils.join(out.get(0), ","));
   * Assert.assertEquals("ss2s:three,three", StringUtils.join(out.get(1), ","));
   * Assert.assertEquals("ss2s:four,four", StringUtils.join(out.get(2), ","));
   * </pre>
   *
   * @return
   * @throws LocalRunException
   */
  public List<Object[]> yield() throws LocalRunException {
    for (InputSource inputSource : inputSources) {
      Object[] data;
      try {
        while ((data = inputSource.getNextRow()) != null) {
          feed(data);
        }
      } catch (IOException e) {
        close();
        throw new LocalRunException(e);
      }
    }

    try {
      return internalYield();
    } finally {
      close();
    }

  }
  
  protected abstract List<Object[]> internalYield() throws LocalRunException;

  /**
   * 将输出结果与用户期望值进行比较，
   * 调用此方法后将不能再次调用feed及feedAll,
   * 如果需要添加更多case需要重新构造runner
   *
   * <pre>
   *
   *  Object[][] inputs = new Object[3][];
   *  inputs[0] = new Object[] { "one", "one" };
   *  inputs[1] = new Object[] { "three", "three" };
   *  inputs[2] = new Object[] { "four", "four" };
   *
   * Object[][] expectedOutputs = new Object[3][];
   * expectedOutputs[0]=new Object[] { "ss2s:one,one" };
   * expectedOutputs[1]=new Object[] { "ss2s:three,three" };
   * expectedOutputs[2]=new Object[] { "ss2s:four,four" };
   *
   * BaseRunner runner = new UDFRunner(odps, new UdfExample());
   * runner.feedAll(inputs).runTest(expectedOutputs);
   *
   * </pre>
   *
   * @param expectedOutputs
   * @throws LocalRunException
   */
  public void runTest(Object[][] expectedOutputs) throws LocalRunException {
    List<Object[]> yields = yield();
    if (expectedOutputs == null && yields == null) {
      return;
    } else if (expectedOutputs == null) {
      org.junit.Assert.fail("expected: null,but was: not null");
    } else if (yields == null) {
      org.junit.Assert.fail("expected: not null,but was: null");
    }

    int count = expectedOutputs.length;
    if (count != yields.size()) {
      org.junit.Assert.fail("expected size:" + count + ",but wase:" + yields.size());
    }

    for (int i = 0; i < count; ++i) {
      Assert.assertArrayEquals("Row number(start with 0):" + i + " ", expectedOutputs[i],
                               yields.get(i));
    }

  }

  /**
   * 将输出结果与用户期望值进行比较，
   * 调用此方法后将不能再次调用feed及feedAll,
   * 如果需要添加更多case需要重新构造runner
   *
   * <pre>
   *
   * List<Object[]> inputs = new ArrayList<Object[]>();
   * inputs.add(new Object[] { "one", "one" });
   * inputs.add(new Object[] { "three", "three" });
   * inputs.add(new Object[] { "four", "four" });
   *
   * List<Object[]> expectedOutputs = new ArrayList<Object[]>();
   * expectedOutputs.add(new Object[] { "ss2s:one,one" });
   * expectedOutputs.add(new Object[] { "ss2s:three,three" });
   * expectedOutputs.add(new Object[] { "ss2s:four,four" });
   *
   * BaseRunner runner = new UDFRunner(odps, new UdfExample());
   * runner.feedAll(inputs).runTest(expectedOutputs);
   *
   * </pre>
   *
   * @param expectedOutputs
   * @throws LocalRunException
   */
  public void runTest(List<Object[]> expectedOutputs) throws LocalRunException {
    if (expectedOutputs == null) {
      List<Object[]> yields = yield();
      if (expectedOutputs == null && yields == null) {
        return;
      } else if (expectedOutputs == null) {
        org.junit.Assert.fail("expected: null,but was: not null");
      } else if (yields == null) {
        org.junit.Assert.fail("expected: not null,but was: null");
      }
    } else {
      Object[][] expected = new Object[expectedOutputs.size()][];
      expectedOutputs.toArray(expected);
      runTest(expected);
    }
  }
  
  /**
   * 程序退出之前（正常退出或抛出异常）必须调用该方法，确保资源得到释放
   * 
   */
  final synchronized protected void close() {
    if (hasClosed) {
      return;
    }
    for (InputSource inputSource : inputSources) {
      try {
        inputSource.close();
      } catch (Exception ex) {

      }
    }
    hasClosed = true;
  }

}