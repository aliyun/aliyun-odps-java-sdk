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

package com.aliyun.odps.conf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import com.aliyun.odps.Survey;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableUtils;

/**
 * 配置类，提供读取和设置配置参数的方法.
 *
 * <h4 id="Resources">配置文件</h4>
 * <p>
 * 支持从配置文件中读取配置信息或写出配置信息，配置文件是标准的 XML，包含一组 name/value 对，示例如下：
 *
 * <pre>
 * &lt;configuration&gt;
 *  &lt;property&gt;
 *   &lt;name&gt;com.mycomp.xxx&lt;/name&gt;
 *   &lt;value&gt;xxx&lt;/value&gt;
 *  &lt;/property&gt;
 *  ... ...
 * &lt;/configuration&gt;
 * </pre>
 *
 * 通过{@link #writeXml(OutputStream)} 可以将配置信息输入到文件。
 * </p>
 *
 * <h4 id="VariableExpansion">变量引用</h4>
 * <p>
 * 配置项的值可以使用变量引用，例如：${user.name}，变量替换的规则：
 * <ol>
 * <li>首先，检查变量名是否是 {@link System#getProperties()} 的系统参数，如果是，则使用系统参数的取值。
 * <li>如果变量名不是系统参数， 检查是否已定义的配置项，如果是，则使用已定义配置项的取值。</li>
 * </ol>
 *
 * <p>
 * 变量引用示例：
 *
 * <pre>
 *  &lt;property&gt;
 *    &lt;name&gt;basedir&lt;/name&gt;
 *    &lt;value&gt;/user/${<i>user.name</i>}&lt;/value&gt;
 *  &lt;/property&gt;
 *
 *  &lt;property&gt;
 *    &lt;name&gt;tempdir&lt;/name&gt;
 *    &lt;value&gt;${<i>basedir</i>}/tmp&lt;/value&gt;
 *  &lt;/property&gt;
 * </pre>
 */
public class Configuration implements Iterable<Map.Entry<String, String>>, Writable {

  static class StringUtils {

    /**
     * Returns a collection of strings.
     *
     * @param str
     *     comma seperated string values
     * @return an <code>ArrayList</code> of string values
     */
    static Collection<String> getStringCollection(String str) {
      List<String> values = new ArrayList<String>();
      if (str == null) {
        return values;
      }
      StringTokenizer tokenizer = new StringTokenizer(str, ",");
      values = new ArrayList<String>();
      while (tokenizer.hasMoreTokens()) {
        values.add(tokenizer.nextToken());
      }
      return values;
    }

    /**
     * Returns an arraylist of strings.
     *
     * @param str
     *     the comma seperated string values
     * @return the arraylist of the comma seperated string values
     */
    public static String[] getStrings(String str) {
      Collection<String> values = getStringCollection(str);
      if (values.size() == 0) {
        return null;
      }
      return values.toArray(new String[values.size()]);
    }

    /**
     * Given an array of strings, return a comma-separated list of its elements.
     *
     * @param strs
     *     Array of strings
     * @return Empty string if strs.length is 0, comma separated list of strings otherwise
     */

    public static String arrayToString(String[] strs) {
      if (strs.length == 0) {
        return "";
      }
      StringBuffer sbuf = new StringBuffer();
      sbuf.append(strs[0]);
      for (int idx = 1; idx < strs.length; idx++) {
        sbuf.append(",");
        sbuf.append(strs[idx]);
      }
      return sbuf.toString();
    }
  }

  private static final Log LOG = LogFactory.getLog(Configuration.class);

  private boolean quietmode = true;

  /**
   * List of configuration resources.
   */
  private ArrayList<Object> resources = new ArrayList<Object>();

  /**
   * List of configuration parameters marked <b>final</b>.
   */
  private Set<String> finalParameters = new HashSet<String>();

  private boolean loadDefaults = true;

  /**
   * Configurtion objects
   */
  private static final WeakHashMap<Configuration, Object> REGISTRY =
      new WeakHashMap<Configuration, Object>();

  /**
   * List of default Resources. Resources are loaded in the order of the list entries
   */
  private static final CopyOnWriteArrayList<String> defaultResources =
      new CopyOnWriteArrayList<String>();

  private Properties properties;
  private Properties overlay;
  private ClassLoader classLoader;

  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Configuration.class.getClassLoader();
    }
  }

  /**
   * 默认Configuration构造方法，这个方法会载入默认配置。
   */
  public Configuration() {
    this(true);
  }

  /**
   * Configuration构造方法，这个方法可以指定是否载入默认配置。如果参数{@code loadDefaults} 是false，构造的实例不会从文件中载入默认配置。
   *
   * @param loadDefaults
   *     指定是否载入默认配置
   */
  public Configuration(boolean loadDefaults) {
    this.loadDefaults = loadDefaults;
    synchronized (Configuration.class) {
      REGISTRY.put(this, null);
    }
  }

  /**
   * Configuration的拷贝构造方法。
   *
   * @param other
   *     拷贝的初始实例
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Configuration(Configuration other) {
    this.resources = (ArrayList) other.resources.clone();
    synchronized (other) {
      if (other.properties != null) {
        this.properties = (Properties) other.properties.clone();
      }

      if (other.overlay != null) {
        this.overlay = (Properties) other.overlay.clone();
      }
    }

    this.finalParameters = new HashSet<String>(other.finalParameters);
    synchronized (Configuration.class) {
      REGISTRY.put(this, null);
    }
  }

  protected void checkState(String name) {
  }

  /**
   * 增加一个默认配置。默认配置是按照增加的顺序来读取的。
   *
   * @param name
   *     配置文件名。这个文件必须在classpath中。
   */
  public static synchronized void addDefaultResource(String name) {
    if (!defaultResources.contains(name)) {
      defaultResources.add(name);
      for (Configuration conf : REGISTRY.keySet()) {
        if (conf.loadDefaults) {
          conf.reloadConfiguration();
        }
      }
    }
  }

  /**
   * 增加一个配置。
   *
   * 新配置会覆盖默认配置，除非配置参数被设定为 <a href="#Final">final</a>.
   *
   * @param name
   *     配置文件名。这个文件必须在classpath中。
   */
  @Survey
  public void addResource(String name) {
    addResourceObject(name);
  }

  /**
   * 增加一个配置。
   *
   * 新配置会覆盖默认配置，除非配置参数被设定为 <a href="#Final">final</a>.
   *
   * @param url
   *     配置文件的url名。会从指定url的本地文件系统中获取配置文件。
   */
  @Survey
  public void addResource(URL url) {
    addResourceObject(url);
  }


  /**
   * 增加一个配置。
   *
   * 新配置会覆盖默认配置，除非配置参数被设定为 <a href="#Final">final</a>.
   *
   * @param in
   *     配置文件的输入流。
   */
  @Survey
  public void addResource(InputStream in) {
    addResourceObject(in);
  }

  /**
   * 从已经添加的资源中重新载入{@link Configuration}。
   *
   * 这个方法会清空所有配置，并从配置文件重新载入一份新的配置。
   */
  public synchronized void reloadConfiguration() {
    properties = null; // trigger reload
    finalParameters.clear(); // clear site-limits
  }

  private synchronized void addResourceObject(Object resource) {
    resources.add(resource); // add to resources
    reloadConfiguration();
  }

  private static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
  private static int MAX_SUBST = 20;

  private String substituteVars(String expr) {
    if (expr == null) {
      return null;
    }
    Matcher match = varPat.matcher("");
    String eval = expr;
    for (int s = 0; s < MAX_SUBST; s++) {
      match.reset(eval);
      if (!match.find()) {
        return eval;
      }
      String var = match.group();
      var = var.substring(2, var.length() - 1); // remove ${ .. }
      String val = null;
      try {
        val = System.getProperty(var);
      } catch (SecurityException se) {
        LOG.warn("No permission to get system property: " + var);
      }
      if (val == null) {
        val = getRaw(var);
      }
      if (val == null) {
        return eval; // return literal ${var}: var is unbound
      }
      // substitute
      eval = eval.substring(0, match.start()) + val + eval.substring(match.end());
    }
    throw new IllegalStateException("Variable substitution depth too large: " + MAX_SUBST + " "
                                    + expr);
  }

  /**
   * 获取参数<code>name</code>的值，如果不存在，返回<code>null</code>。参数值会使用变量展开。
   *
   * @param name
   *     参数名
   * @return 参数<code>name</code>的字符串值, 如果不存在，返回<code>null</code>。
   */
  public String get(String name) {
    return substituteVars(getProps().getProperty(name));
  }

  /**
   * 获取参数<code>name</code>的值，不使用变量展开。
   *
   * @param name
   *     参数名
   * @return 参数<code>name</code>的值, 如果不存在，返回<code>null</code>。
   */
  public String getRaw(String name) {
    return getProps().getProperty(name);
  }

  /**
   * 设置 参数<code>name</code>的值 <code>value</code> 。
   *
   * @param name
   *     参数名
   * @param value
   *     参数值
   */
  public void set(String name, String value) {
    checkState(name);
    getOverlay().setProperty(name, value);
    getProps().setProperty(name, value);
  }

  /**
   * 如果指定参数不存在则设定它
   *
   * @param name
   *     参数名
   * @param value
   *     参数值
   */
  public void setIfUnset(String name, String value) {
    if (get(name) == null) {
      set(name, value);
    }
  }

  private synchronized Properties getOverlay() {
    if (overlay == null) {
      overlay = new Properties();
    }
    return overlay;
  }

  /**
   * 获取参数<code>name</code>的值，如果不存在，返回<code>defaultValue</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的字符串值，如果不存在，返回<code>defaultValue</code>。
   */
  public String get(String name, String defaultValue) {
    return substituteVars(getProps().getProperty(name, defaultValue));
  }

  /**
   * 获取参数<code>name</code>的整形值，如果不存在，返回<code>defaultValue</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的整形值，如果不存在，返回<code>defaultValue</code>。
   */
  public int getInt(String name, int defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      String hexString = getHexDigits(valueString);
      if (hexString != null) {
        return Integer.parseInt(hexString, 16);
      }
      return Integer.parseInt(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * 设定参数 <code>name</code> 的整形值
   *
   * @param name
   *     参数名
   * @param value
   *     整形的参数值
   */
  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }

  /**
   * 获取参数<code>name</code>的长整形值，如果不存在，返回<code>defaultValue</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的长整型值，如果不存在，返回<code>defaultValue</code>。
   */
  public long getLong(String name, long defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      String hexString = getHexDigits(valueString);
      if (hexString != null) {
        return Long.parseLong(hexString, 16);
      }
      return Long.parseLong(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private String getHexDigits(String value) {
    boolean negative = false;
    String str = value;
    String hexString = null;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }

  /**
   * 设定参数 <code>name</code> 的长整形值
   *
   * @param name
   *     参数名
   * @param value
   *     长整形的参数值
   */
  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  /**
   * 获取参数<code>name</code>的浮点值，如果不存在，返回<code>defaultValue</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的浮点值，如果不存在，返回<code>defaultValue</code>。
   */
  public float getFloat(String name, float defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      return Float.parseFloat(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * 设定参数 <code>name</code> 的浮点值
   *
   * @param name
   *     参数名
   * @param value
   *     浮点形的参数值
   */
  public void setFloat(String name, float value) {
    set(name, Float.toString(value));
  }

  /**
   * 获取参数<code>name</code>的布尔值，如果不存在，返回<code>defaultValue</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的布尔值，如果不存在，返回<code>defaultValue</code>。
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = get(name);
    if ("true".equals(valueString)) {
      return true;
    } else if ("false".equals(valueString)) {
      return false;
    } else {
      return defaultValue;
    }
  }

  /**
   * 设定参数 <code>name</code> 的布尔值
   *
   * @param name
   *     参数名
   * @param value
   *     布尔形的参数值
   */
  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  /**
   * 如果参数 <code>name</code> 未设定，设定它的布尔值
   *
   * @param name
   *     参数名
   * @param value
   *     布尔形的参数值
   */
  public void setBooleanIfUnset(String name, boolean value) {
    setIfUnset(name, Boolean.toString(value));
  }

  /**
   * 表示正整数区间的类。它解析一个描述字符串，例如"2-3,5,7-"，其中','分隔区间，上下限用'-'链接。当忽略上限或下限时，指定对应的区间不受限。
   * 所以上面的例子指定的区间是2，3，5，7，8，9，...。
   */
  static class IntegerRanges {

    private static class Range {

      int start;
      int end;
    }

    List<Range> ranges = new ArrayList<Range>();

    public IntegerRanges() {
    }

    public IntegerRanges(String newValue) {
      StringTokenizer itr = new StringTokenizer(newValue, ",");
      while (itr.hasMoreTokens()) {
        String rng = itr.nextToken().trim();
        String[] parts = rng.split("-", 3);
        if (parts.length < 1 || parts.length > 2) {
          throw new IllegalArgumentException("integer range badly formed: " + rng);
        }
        Range r = new Range();
        r.start = convertToInt(parts[0], 0);
        if (parts.length == 2) {
          r.end = convertToInt(parts[1], Integer.MAX_VALUE);
        } else {
          r.end = r.start;
        }
        if (r.start > r.end) {
          throw new IllegalArgumentException("IntegerRange from " + r.start + " to " + r.end
                                             + " is invalid");
        }
        ranges.add(r);
      }
    }

    /**
     * Convert a string to an int treating empty strings as the default value.
     *
     * @param value
     *     the string value
     * @param defaultValue
     *     the value for if the string is empty
     * @return the desired integer
     */
    private static int convertToInt(String value, int defaultValue) {
      String trim = value.trim();
      if (trim.length() == 0) {
        return defaultValue;
      }
      return Integer.parseInt(trim);
    }

    /**
     * Is the given value in the set of ranges
     *
     * @param value
     *     the value to check
     * @return is the value in the ranges?
     */
    public boolean isIncluded(int value) {
      for (Range r : ranges) {
        if (r.start <= value && value <= r.end) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuffer result = new StringBuffer();
      boolean first = true;
      for (Range r : ranges) {
        if (first) {
          first = false;
        } else {
          result.append(',');
        }
        result.append(r.start);
        result.append('-');
        result.append(r.end);
      }
      return result.toString();
    }
  }

  /**
   * 获取参数<code>name</code>的值为一个区间，如果不存在，返回<code>defaultValue</code>指定的区间。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的数字区间
   */
  public IntegerRanges getRange(String name, String defaultValue) {
    return new IntegerRanges(get(name, defaultValue));
  }

  /**
   * 获取参数<code>name</code>的字符值集合，如果不存在，返回<code>null</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的字符数组值，如果不存在，返回<code>null</code>。
   */
  public Collection<String> getStringCollection(String name) {
    String valueString = get(name);
    return StringUtils.getStringCollection(valueString);
  }

  /**
   * 获取参数<code>name</code>的字符数组值，如果不存在，返回<code>null</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的字符数组值，如果不存在，返回<code>null</code>。
   */
  public String[] getStrings(String name) {
    String valueString = get(name);
    return StringUtils.getStrings(valueString);
  }

  /**
   * 获取参数<code>name</code>的字符数组值，如果不存在，返回<code>defaultValue</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>的字符数组值，如果不存在，返回<code>defaultValue</code>。
   */
  public String[] getStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    } else {
      return StringUtils.getStrings(valueString);
    }
  }

  /**
   * 设置 参数<code>name</code> 的字符串数组值 <code>value</code> 。
   *
   * @param name
   *     参数名
   * @param values
   *     参数值
   */
  public void setStrings(String name, String... values) {
    set(name, StringUtils.arrayToString(values));
  }

  /**
   * 根据类名载入Java类
   *
   * @param name
   *     类名
   * @return 载入类的实例
   * @throws ClassNotFoundException
   *     如果找不到指定的Java类
   */
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    return Class.forName(name, false, classLoader);
  }

  /**
   * 获取参数<code>name</code>指定的类对象数组，如果不存在，返回<code>defaultValue</code>。 如果类未能找到，则会抛出RuntimeException。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>指定的类对象数组，如果参数未指定，返回<code>defaultValue</code>
   */
  public Class<?>[] getClasses(String name, Class<?>... defaultValue) {
    String[] classnames = getStrings(name);
    if (classnames == null) {
      return defaultValue;
    }
    try {
      Class<?>[] classes = new Class<?>[classnames.length];
      for (int i = 0; i < classnames.length; i++) {
        classes[i] = getClassByName(classnames[i]);
      }
      return classes;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 获取参数<code>name</code>指定的类对象，如果不存在，返回<code>defaultValue</code>。 如果类未能找到，则会抛出RuntimeException。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @return 参数<code>name</code>指定的类对象，如果参数未指定，返回<code>defaultValue</code>
   */
  public Class<?> getClass(String name, Class<?> defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      return getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("ODPS-0730001: ClassNotFoundException - " + e.getMessage());
    }
  }

  /**
   * 获取参数<code>name</code>指定的类对象，这个类实现的接口由 <code>xface</code>指定。
   *
   * 如果不存在，返回<code>defaultValue</code>。
   *
   * @param name
   *     参数名
   * @param defaultValue
   *     默认值
   * @param xface
   *     指定类实现的接口
   * @return 参数<code>name</code>指定的类对象, 如果参数未指定，返回 <code>defaultValue</code>
   */
  public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue,
                                         Class<U> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass)) {
        throw new RuntimeException(theClass + " not " + xface.getName());
      } else if (theClass != null) {
        return theClass.asSubclass(xface);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 设置<code>name</code>参数指定的类，这个类实现的接口由 <code>xface</code>指定。
   *
   * @param name
   *     参数名
   * @param theClass
   *     参数值
   * @param xface
   *     指定的类实现的接口
   */
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    if (!xface.isAssignableFrom(theClass)) {
      throw new RuntimeException(theClass + " not " + xface.getName());
    }
    set(name, theClass.getName());
  }

  /**
   * 获取一个本地文件，其父目录由名称为 <i>dirsProp</i> 的参数指定，路径为 <i>path</i>. 如果 <i>dirsProp</i> 包含多个目录,
   * 则返回其中的一个文件。
   * 如果指定的文件不存在则创建它。
   *
   * @param dirsProp
   *     directory in which to locate the file.
   * @param path
   *     file-path.
   * @return local file under the directory with the given path.
   */
  public File getFile(String dirsProp, String path) throws IOException {
    String[] dirs = getStrings(dirsProp);
    int hashCode = path.hashCode();
    for (int i = 0; i < dirs.length; i++) { // try each local dir
      int index = (hashCode + i & Integer.MAX_VALUE) % dirs.length;
      File file = new File(dirs[index], path);
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;
      }
    }
    throw new IOException("No valid local directories in property: " + dirsProp);
  }

  /**
   * 获取指定配置文件的{@link URL}
   *
   * @param name
   *     配置文件名
   * @return 指定配置文件的{@link URL}
   */
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }

  /**
   * 获取一个指定配置文件的 {@link InputStream}
   *
   * @param name
   *     配置文件名
   * @return 指定配置文件的{@link InputStream}
   */
  public InputStream getConfResourceAsInputStream(String name) {
    try {
      URL url = getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * 获取一个指定配置文件的 {@link Reader}
   *
   * @param name
   *     配置文件名
   * @return 指定配置文件的{@link Reader}
   */
  public Reader getConfResourceAsReader(String name) {
    try {
      URL url = getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new InputStreamReader(url.openStream());
    } catch (Exception e) {
      return null;
    }
  }

  private synchronized Properties getProps() {
    if (properties == null) {
      properties = new Properties();
      loadResources(properties, resources, quietmode);
      if (overlay != null) {
        properties.putAll(overlay);
      }
    }
    return properties;
  }

  /**
   * 配置项的计数
   *
   * @return 配置项的计数
   */
  public int size() {
    return getProps().size();
  }

  /**
   * 清除所有配置
   */
  public void clear() {
    getProps().clear();
    getOverlay().clear();
  }

  /**
   * 所有配置的迭代器，格式为key/value。key和value的类型都为string。
   *
   * @return 配置的迭代器
   */
  public Iterator<Map.Entry<String, String>> iterator() {
    // Get a copy of just the string to string pairs. After the old object
    // methods that allow non-strings to be put into configurations are
    // removed,
    // we could replace properties with a Map<String,String> and get rid of
    // this
    // code.
    Map<String, String> result = new HashMap<String, String>();
    for (Map.Entry<Object, Object> item : getProps().entrySet()) {
      if (item.getKey() instanceof String && item.getValue() instanceof String) {
        result.put((String) item.getKey(), (String) item.getValue());
      }
    }
    return result.entrySet().iterator();
  }

  @SuppressWarnings("rawtypes")
  private void loadResources(Properties properties, ArrayList resources, boolean quiet) {
    if (loadDefaults) {
      for (String resource : defaultResources) {
        loadResource(properties, resource, quiet);
      }
    }

    for (Object resource : resources) {
      loadResource(properties, resource, quiet);
    }
  }

  private void loadResource(Properties properties, Object name, boolean quiet) {
    try {
      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
      // ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);

      // allow includes in the xml file
      docBuilderFactory.setNamespaceAware(true);
      try {
        docBuilderFactory.setXIncludeAware(true);
      } catch (UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for parser " + docBuilderFactory + ":" + e,
                  e);
      }
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = null;
      Element root = null;

      if (name instanceof URL) { // an URL resource
        URL url = (URL) name;
        if (url != null) {
          if (!quiet) {
            LOG.info("parsing " + url);
          }
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof String) { // a CLASSPATH resource
        URL url = getResource((String) name);
        if (url != null) {
          if (!quiet) {
            LOG.info("parsing " + url);
          }
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof InputStream) {
        try {
          doc = builder.parse((InputStream) name);
        } finally {
          ((InputStream) name).close();
        }
      } else if (name instanceof Element) {
        root = (Element) name;
      }

      if (doc == null && root == null) {
        if (quiet) {
          return;
        }
        throw new RuntimeException(name + " not found");
      }

      if (root == null) {
        root = doc.getDocumentElement();
      }
      if (!"configuration".equals(root.getTagName())) {
        LOG.fatal("bad conf file: top-level element not <configuration>");
      }
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element)) {
          continue;
        }
        Element prop = (Element) propNode;
        if ("configuration".equals(prop.getTagName())) {
          loadResource(properties, prop, quiet);
          continue;
        }
        if (!"property".equals(prop.getTagName())) {
          LOG.warn("bad conf file: element not <property>");
        }
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        boolean finalParameter = false;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element)) {
            continue;
          }
          Element field = (Element) fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
            attr = ((Text) field.getFirstChild()).getData().trim();
          }
          if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
            value = ((Text) field.getFirstChild()).getData();
          }
          if ("final".equals(field.getTagName()) && field.hasChildNodes()) {
            finalParameter = "true".equals(((Text) field.getFirstChild()).getData());
          }
        }

        // Ignore this parameter if it has already been marked as
        // 'final'
        if (attr != null && value != null) {
          if (!finalParameters.contains(attr)) {
            properties.setProperty(attr, value);
            if (finalParameter) {
              finalParameters.add(attr);
            }
          } else {
            LOG.warn(name + ":a attempt to override final parameter: " + attr + ";  Ignoring.");
          }
        }
      }

    } catch (IOException e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (DOMException e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (SAXException e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (ParserConfigurationException e) {
      LOG.fatal("error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
  }

  /**
   * 写出所有非默认的参数到一个 {@link OutputStream}
   *
   * @param out
   *     输出的{@link OutputStream}
   */
  @SuppressWarnings("rawtypes")
  public void writeXml(OutputStream out) throws IOException {
    Properties properties = getProps();
    try {
      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
      Element conf = doc.createElement("configuration");
      doc.appendChild(conf);
      conf.appendChild(doc.createTextNode("\n"));
      for (Enumeration e = properties.keys(); e.hasMoreElements(); ) {
        String name = (String) e.nextElement();
        Object object = properties.get(name);
        String value = null;
        if (object instanceof String) {
          value = (String) object;
        } else {
          continue;
        }
        Element propNode = doc.createElement("property");
        conf.appendChild(propNode);

        Element nameNode = doc.createElement("name");
        nameNode.appendChild(doc.createTextNode(name));
        propNode.appendChild(nameNode);

        Element valueNode = doc.createElement("value");
        valueNode.appendChild(doc.createTextNode(value));
        propNode.appendChild(valueNode);

        conf.appendChild(doc.createTextNode("\n"));
      }

      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.transform(source, result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 获取{@link ClassLoader}
   *
   * @return {@link ClassLoader}对象
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * 设置{@link ClassLoader}对象
   *
   * @param classLoader
   *     新的{@link ClassLoader}对象
   */
  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Configuration: ");
    if (loadDefaults) {
      toString(defaultResources, sb);
      if (resources.size() > 0) {
        sb.append(", ");
      }
    }
    toString(resources, sb);
    return sb.toString();
  }

  @SuppressWarnings("rawtypes")
  private void toString(List resources, StringBuffer sb) {
    ListIterator i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(", ");
      }
      sb.append(i.next());
    }
  }

  /**
   * 设定quiet模式。quiet模式下，错误等信息不会记录在日志里。
   *
   * @param quietmode
   *     <code>true</code> 打开， <code>false</code> 关闭。
   */
  public synchronized void setQuietMode(boolean quietmode) {
    this.quietmode = quietmode;
  }

  /**
   * 从 {@link DataInput} 读取
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    clear();
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; ++i) {
      set(com.aliyun.odps.io.Text.readString(in),
          com.aliyun.odps.io.Text.readString(in));
    }
  }

  /**
   * 输出到 {@link DataOutput}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Properties props = getProps();
    WritableUtils.writeVInt(out, props.size());
    for (Map.Entry<Object, Object> item : props.entrySet()) {
      com.aliyun.odps.io.Text.writeString(out, (String) item.getKey());
      com.aliyun.odps.io.Text.writeString(out, (String) item.getValue());
    }
  }

}
