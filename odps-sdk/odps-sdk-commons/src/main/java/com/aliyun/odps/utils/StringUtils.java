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

package com.aliyun.odps.utils;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

/**
 * General string utils
 */
public class StringUtils {

  private static final DecimalFormat decimalFormat;

  private static final BitSet PRINTABLE_CHARS = new BitSet(256);

  private static final byte PRINTABLE_ESCAPE_CHAR = '=';

  static {
    NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.ENGLISH);
    decimalFormat = (DecimalFormat) numberFormat;
    decimalFormat.applyPattern("#.##");

    // Static initializer for printable chars collection
    for (int i = 33; i <= 60; i++) {
      // alpha characters
      PRINTABLE_CHARS.set(i);
    }
    // ignore 61(=)
    for (int i = 62; i <= 123; i++) {
      PRINTABLE_CHARS.set(i);
    }
    // ignore 127(DEL) and 124(|)
    PRINTABLE_CHARS.set(125);
    PRINTABLE_CHARS.set(126);
  }

  /**
   * Make a string representation of the exception.
   *
   * @param e
   *     The exception to stringify
   * @return A string with exception name and call stack.
   */
  public static String stringifyException(Throwable e) {
    StringWriter stm = new StringWriter();
    PrintWriter wrt = new PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }

  /**
   * Given a full hostname, return the word upto the first dot.
   *
   * @param fullHostname
   *     the full hostname
   * @return the hostname to the first dot
   */
  public static String simpleHostname(String fullHostname) {
    int offset = fullHostname.indexOf('.');
    if (offset != -1) {
      return fullHostname.substring(0, offset);
    }
    return fullHostname;
  }

  private static DecimalFormat oneDecimal = new DecimalFormat("0.0");

  /**
   * Given an integer, return a string that is in an approximate, but human
   * readable format. It uses the bases 'k', 'm', and 'g' for 1024, 1024**2, and
   * 1024**3.
   *
   * @param number
   *     the number to format
   * @return a human readable form of the integer
   */
  public static String humanReadableInt(long number) {
    long absNumber = Math.abs(number);
    double result = number;
    String suffix = "";
    if (absNumber < 1024) {
      return String.valueOf(number);
    } else if (absNumber < 1024 * 1024) {
      result = number / 1024.0;
      suffix = "K";
    } else if (absNumber < 1024 * 1024 * 1024) {
      result = number / (1024.0 * 1024);
      suffix = "M";
    } else {
      result = number / (1024.0 * 1024 * 1024);
      suffix = "G";
    }
    return oneDecimal.format(result) + suffix;
  }

  /**
   * Format a percentage for presentation to the user.
   *
   * @param done
   *     the percentage to format (0.0 to 1.0)
   * @param digits
   *     the number of digits past the decimal point
   * @return a string representation of the percentage
   */
  public static String formatPercent(double done, int digits) {
    DecimalFormat percentFormat = new DecimalFormat("0.00%");
    double scale = Math.pow(10.0, digits + 2);
    double rounded = Math.floor(done * scale);
    percentFormat.setDecimalSeparatorAlwaysShown(false);
    percentFormat.setMinimumFractionDigits(digits);
    percentFormat.setMaximumFractionDigits(digits);
    return percentFormat.format(rounded / scale);
  }

  /**
   * Given an array of strings, return a comma-separated list of its elements.
   *
   * @param strs
   *     Array of strings
   * @return Empty string if strs.length is 0, comma separated list of strings
   * otherwise
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

  /**
   * Given an array of bytes it will convert the bytes to a hex string
   * representation of the bytes
   *
   * @param bytes
   * @param start
   *     start index, inclusively
   * @param end
   *     end index, exclusively
   * @return hex string representation of the byte array
   */
  public static String byteToHexString(byte[] bytes, int start, int end) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes == null");
    }
    StringBuilder s = new StringBuilder();
    for (int i = start; i < end; i++) {
      s.append(String.format("%02x", bytes[i]));
    }
    return s.toString();
  }

  /**
   * Same as byteToHexString(bytes, 0, bytes.length).
   */
  public static String byteToHexString(byte bytes[]) {
    return byteToHexString(bytes, 0, bytes.length);
  }

  /**
   * Given a hexstring this will return the byte array corresponding to the
   * string
   *
   * @param hex
   *     the hex String array
   * @return a byte array that is a hex string representation of the given
   * string. The size of the byte array is therefore hex.length/2
   */
  public static byte[] hexStringToByte(String hex) {
    byte[] bts = new byte[hex.length() / 2];
    for (int i = 0; i < bts.length; i++) {
      bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    return bts;
  }

  /**
   * @param uris
   */
  public static String uriToString(URI[] uris) {
    if (uris == null) {
      return null;
    }
    StringBuffer ret = new StringBuffer(uris[0].toString());
    for (int i = 1; i < uris.length; i++) {
      ret.append(",");
      ret.append(uris[i].toString());
    }
    return ret.toString();
  }

  /**
   * @param str
   */
  public static URI[] stringToURI(String[] str) {
    if (str == null) {
      return null;
    }
    URI[] uris = new URI[str.length];
    for (int i = 0; i < str.length; i++) {
      try {
        uris[i] = new URI(str[i]);
      } catch (URISyntaxException ur) {
        System.out.println("Exception in specified URI's "
                           + StringUtils.stringifyException(ur));
        // making sure its asssigned to null in case of an error
        uris[i] = null;
      }
    }
    return uris;
  }

  /**
   * Given a finish and start time in long milliseconds, returns a String in the
   * format Xhrs, Ymins, Z sec, for the time difference between two times. If
   * finish time comes before start time then negative valeus of X, Y and Z wil
   * return.
   *
   * @param finishTime
   *     finish time
   * @param startTime
   *     start time
   */
  public static String formatTimeDiff(long finishTime, long startTime) {
    long timeDiff = finishTime - startTime;
    return formatTime(timeDiff);
  }

  /**
   * Given the time in long milliseconds, returns a String in the format Xhrs,
   * Ymins, Z sec.
   *
   * @param timeDiff
   *     The time difference to format
   */
  public static String formatTime(long timeDiff) {
    StringBuffer buf = new StringBuffer();
    long hours = timeDiff / (60 * 60 * 1000);
    long rem = (timeDiff % (60 * 60 * 1000));
    long minutes = rem / (60 * 1000);
    rem = rem % (60 * 1000);
    long seconds = rem / 1000;

    if (hours != 0) {
      buf.append(hours);
      buf.append("hrs, ");
    }
    if (minutes != 0) {
      buf.append(minutes);
      buf.append("mins, ");
    }
    // return "0sec if no difference
    buf.append(seconds);
    buf.append("sec");
    return buf.toString();
  }

  /**
   * Formats time in ms and appends difference (finishTime - startTime) as
   * returned by formatTimeDiff(). If finish time is 0, empty string is
   * returned, if start time is 0 then difference is not appended to return
   * value.
   *
   * @param dateFormat
   *     date format to use
   * @param finishTime
   *     fnish time
   * @param startTime
   *     start time
   * @return formatted value.
   */
  public static String getFormattedTimeWithDiff(DateFormat dateFormat,
                                                long finishTime, long startTime) {
    StringBuffer buf = new StringBuffer();
    if (0 != finishTime) {
      buf.append(dateFormat.format(new Date(finishTime)));
      if (0 != startTime) {
        buf.append(" (" + formatTimeDiff(finishTime, startTime) + ")");
      }
    }
    return buf.toString();
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
   * Returns a collection of strings.
   *
   * @param str
   *     comma seperated string values
   * @return an <code>ArrayList</code> of string values
   */
  public static Collection<String> getStringCollection(String str) {
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

  final public static char COMMA = ',';
  final public static String COMMA_STR = ",";
  final public static char ESCAPE_CHAR = '\\';

  /**
   * Split a string using the default separator
   *
   * @param str
   *     a string that may have escaped separator
   * @return an array of strings
   */
  public static String[] split(String str) {
    return split(str, ESCAPE_CHAR, COMMA);
  }

  /**
   * Split a string using the given separator
   *
   * @param str
   *     a string that may have escaped separator
   * @param escapeChar
   *     a char that be used to escape the separator
   * @param separator
   *     a separator char
   * @return an array of strings
   */
  public static String[] split(String str, char escapeChar, char separator) {
    if (str == null) {
      return null;
    }
    ArrayList<String> strList = new ArrayList<String>();
    StringBuilder split = new StringBuilder();
    int index = 0;
    while ((index = findNext(str, separator, escapeChar, index, split)) >= 0) {
      ++index; // move over the separator for next search
      strList.add(split.toString());
      split.setLength(0); // reset the buffer
    }
    strList.add(split.toString());
    // remove trailing empty split(s)
    int last = strList.size(); // last split
    while (--last >= 0 && "".equals(strList.get(last))) {
      strList.remove(last);
    }
    return strList.toArray(new String[strList.size()]);
  }
  
  public static String[] split(String str, char separatorChar) {
      return splitWorker(str, separatorChar, false);
  }

  /**
   * Finds the first occurrence of the separator character ignoring the escaped
   * separators starting from the index. Note the substring between the index
   * and the position of the separator is passed.
   *
   * @param str
   *     the source string
   * @param separator
   *     the character to find
   * @param escapeChar
   *     character used to escape
   * @param start
   *     from where to search
   * @param split
   *     used to pass back the extracted string
   */
  public static int findNext(String str, char separator, char escapeChar,
                             int start, StringBuilder split) {
    int numPreEscapes = 0;
    for (int i = start; i < str.length(); i++) {
      char curChar = str.charAt(i);
      if (numPreEscapes == 0 && curChar == separator) { // separator
        return i;
      } else {
        split.append(curChar);
        numPreEscapes = (curChar == escapeChar) ? (++numPreEscapes) % 2 : 0;
      }
    }
    return -1;
  }

  /**
   * Escape commas in the string using the default escape char
   *
   * @param str
   *     a string
   * @return an escaped string
   */
  public static String escapeString(String str) {
    return escapeString(str, ESCAPE_CHAR, COMMA);
  }

  /**
   * Escape <code>charToEscape</code> in the string with the escape char
   * <code>escapeChar</code>
   *
   * @param str
   *     string
   * @param escapeChar
   *     escape char
   * @param charToEscape
   *     the char to be escaped
   * @return an escaped string
   */
  public static String escapeString(String str, char escapeChar,
                                    char charToEscape) {
    return escapeString(str, escapeChar, new char[]{charToEscape});
  }

  // check if the character array has the character
  private static boolean hasChar(char[] chars, char character) {
    for (char target : chars) {
      if (character == target) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param charsToEscape
   *     array of characters to be escaped
   */
  public static String escapeString(String str, char escapeChar,
                                    char[] charsToEscape) {
    if (str == null) {
      return null;
    }
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < str.length(); i++) {
      char curChar = str.charAt(i);
      if (curChar == escapeChar || hasChar(charsToEscape, curChar)) {
        // special char
        result.append(escapeChar);
      }
      result.append(curChar);
    }
    return result.toString();
  }

  /**
   * Unescape commas in the string using the default escape char
   *
   * @param str
   *     a string
   * @return an unescaped string
   */
  public static String unEscapeString(String str) {
    return unEscapeString(str, ESCAPE_CHAR, COMMA);
  }

  /**
   * Unescape <code>charToEscape</code> in the string with the escape char
   * <code>escapeChar</code>
   *
   * @param str
   *     string
   * @param escapeChar
   *     escape char
   * @param charToEscape
   *     the escaped char
   * @return an unescaped string
   */
  public static String unEscapeString(String str, char escapeChar,
                                      char charToEscape) {
    return unEscapeString(str, escapeChar, new char[]{charToEscape});
  }

  /**
   * @param charsToEscape
   *     array of characters to unescape
   */
  public static String unEscapeString(String str, char escapeChar,
                                      char[] charsToEscape) {
    if (str == null) {
      return null;
    }
    StringBuilder result = new StringBuilder(str.length());
    boolean hasPreEscape = false;
    for (int i = 0; i < str.length(); i++) {
      char curChar = str.charAt(i);
      if (hasPreEscape) {
        if (curChar != escapeChar && !hasChar(charsToEscape, curChar)) {
          // no special char
          throw new IllegalArgumentException("Illegal escaped string " + str
                                             + " unescaped " + escapeChar + " at " + (i - 1));
        }
        // otherwise discard the escape char
        result.append(curChar);
        hasPreEscape = false;
      } else {
        if (hasChar(charsToEscape, curChar)) {
          throw new IllegalArgumentException("Illegal escaped string " + str
                                             + " unescaped " + curChar + " at " + i);
        } else if (curChar == escapeChar) {
          hasPreEscape = true;
        } else {
          result.append(curChar);
        }
      }
    }
    if (hasPreEscape) {
      throw new IllegalArgumentException("Illegal escaped string " + str
                                         + ", not expecting " + escapeChar + " in the end.");
    }
    return result.toString();
  }

  /**
   * Return hostname without throwing exception.
   *
   * @return hostname
   */
  public static String getHostname() {
    try {
      return "" + InetAddress.getLocalHost();
    } catch (UnknownHostException uhe) {
      return "" + uhe;
    }
  }

  /**
   * Return a message for logging.
   *
   * @param prefix
   *     prefix keyword for the message
   * @param msg
   *     content of the message
   * @return a message for logging
   */
  private static String toStartupShutdownString(String prefix, String[] msg) {
    StringBuffer b = new StringBuffer(prefix);
    b.append("\n/************************************************************");
    for (String s : msg) {
      b.append("\n" + prefix + s);
    }
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * Print a log message for starting up and shutting down
   *
   * @param clazz
   *     the class of the server
   * @param args
   *     arguments
   * @param LOG
   *     the target log object
   */
  public static void startupShutdownMessage(Class<?> clazz, String[] args,
                                            final org.apache.commons.logging.Log LOG) {
    final String hostname = getHostname();
    final String classname = clazz.getSimpleName();
    LOG.info(toStartupShutdownString("STARTUP_MSG: ",
                                     new String[]{"Starting " + classname, "  host = " + hostname,
                                                  "  args = " + Arrays.asList(args)}));

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ",
                                         new String[]{
                                             "Shutting down " + classname + " at " + hostname}));
      }
    });
  }

  /**
   * The traditional binary prefixes, kilo, mega, ..., exa, which can be
   * represented by a 64-bit integer. TraditionalBinaryPrefix symbol are case
   * insensitive.
   */
  public static enum TraditionalBinaryPrefix {
    KILO(1024), MEGA(KILO.value << 10), GIGA(MEGA.value << 10), TERA(
        GIGA.value << 10), PETA(TERA.value << 10), EXA(PETA.value << 10);

    public final long value;
    public final char symbol;

    TraditionalBinaryPrefix(long value) {
      this.value = value;
      this.symbol = toString().charAt(0);
    }

    /**
     * @return The TraditionalBinaryPrefix object corresponding to the symbol.
     */
    public static TraditionalBinaryPrefix valueOf(char symbol) {
      symbol = Character.toUpperCase(symbol);
      for (TraditionalBinaryPrefix prefix : TraditionalBinaryPrefix.values()) {
        if (symbol == prefix.symbol) {
          return prefix;
        }
      }
      throw new IllegalArgumentException("Unknown symbol '" + symbol + "'");
    }

    /**
     * Convert a string to long. The input string is first be trimmed and then
     * it is parsed with traditional binary prefix.
     *
     * For example, "-1230k" will be converted to -1230 * 1024 = -1259520;
     * "891g" will be converted to 891 * 1024^3 = 956703965184;
     *
     * @param s
     *     input string
     * @return a long value represented by the input string.
     */
    public static long string2long(String s) {
      s = s.trim();
      final int lastpos = s.length() - 1;
      final char lastchar = s.charAt(lastpos);
      if (Character.isDigit(lastchar)) {
        return Long.parseLong(s);
      } else {
        long prefix = TraditionalBinaryPrefix.valueOf(lastchar).value;
        long num = Long.parseLong(s.substring(0, lastpos));
        if (num > (Long.MAX_VALUE / prefix) || num < (Long.MIN_VALUE / prefix)) {
          throw new IllegalArgumentException(s + " does not fit in a Long");
        }
        return num * prefix;
      }
    }
  }

  /**
   * Escapes HTML Special characters present in the string.
   *
   * @param string
   * @return HTML Escaped String representation
   */
  public static String escapeHTML(String string) {
    if (string == null) {
      return null;
    }
    StringBuffer sb = new StringBuffer();
    boolean lastCharacterWasSpace = false;
    char[] chars = string.toCharArray();
    for (char c : chars) {
      if (c == ' ') {
        if (lastCharacterWasSpace) {
          lastCharacterWasSpace = false;
          sb.append("&nbsp;");
        } else {
          lastCharacterWasSpace = true;
          sb.append(" ");
        }
      } else {
        lastCharacterWasSpace = false;
        switch (c) {
          case '<':
            sb.append("&lt;");
            break;
          case '>':
            sb.append("&gt;");
            break;
          case '&':
            sb.append("&amp;");
            break;
          case '"':
            sb.append("&quot;");
            break;
          default:
            sb.append(c);
            break;
        }
      }
    }

    return sb.toString();
  }

  /**
   * Return an abbreviated English-language desc of the byte length
   */
  public static String byteDesc(long len) {
    double val = 0.0;
    String ending = "";
    if (len < 1024 * 1024) {
      val = (1.0 * len) / 1024;
      ending = " KB";
    } else if (len < 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024 * 1024);
      ending = " MB";
    } else if (len < 1024L * 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024 * 1024 * 1024);
      ending = " GB";
    } else if (len < 1024L * 1024 * 1024 * 1024 * 1024) {
      val = (1.0 * len) / (1024L * 1024 * 1024 * 1024);
      ending = " TB";
    } else {
      val = (1.0 * len) / (1024L * 1024 * 1024 * 1024 * 1024);
      ending = " PB";
    }
    return limitDecimalTo2(val) + ending;
  }

  public static synchronized String limitDecimalTo2(double d) {
    return decimalFormat.format(d);
  }
  
  public static boolean isEmpty(String str) {
      return str == null || str.length() == 0;
  }

  public static boolean isNullOrEmpty(String str) {
    return (str == null) || str.trim().isEmpty();
  }
  
  public static boolean isBlank(String str) {
      int strLen;
      if (str == null || (strLen = str.length()) == 0) {
          return true;
      }
      for (int i = 0; i < strLen; i++) {
          if ((Character.isWhitespace(str.charAt(i)) == false)) {
              return false;
          }
      }
      return true;
  }
  
  public static boolean isNotBlank(String str) {
      return !StringUtils.isBlank(str);
  }

  public static boolean allNotNullOrEmpty(String... str) {
    return Arrays.stream(str).noneMatch(StringUtils::isNullOrEmpty);
  }

  public static String join(Object[] array, char separator) {
      if (array == null) {
          return null;
      }

      return join(array, separator, 0, array.length);
  }
  
  public static String join(Object[] array, char separator, int startIndex, int endIndex) {
      if (array == null) {
          return null;
      }
      int bufSize = (endIndex - startIndex);
      if (bufSize <= 0) {
          return "";
      }

      bufSize *= ((array[startIndex] == null ? 16 : array[startIndex].toString().length()) + 1);
      StringBuffer buf = new StringBuffer(bufSize);

      for (int i = startIndex; i < endIndex; i++) {
          if (i > startIndex) {
              buf.append(separator);
          }
          if (array[i] != null) {
              buf.append(array[i]);
          }
      }
      return buf.toString();
  }
  
  public static String join(Object[] array, String separator) {
      if (array == null) {
          return null;
      }
      return join(array, separator, 0, array.length);
  }
  
  public static String join(Object[] array, String separator, int startIndex, int endIndex) {
      if (array == null) {
          return null;
      }
      if (separator == null) {
          separator = "";
      }

      // endIndex - startIndex > 0:   Len = NofStrings *(len(firstString) + len(separator))
      //           (Assuming that all Strings are roughly equally long)
      int bufSize = (endIndex - startIndex);
      if (bufSize <= 0) {
          return "";
      }

      bufSize *= ((array[startIndex] == null ? 16 : array[startIndex].toString().length())
                      + separator.length());

      StringBuilder buf = new StringBuilder(bufSize);

      for (int i = startIndex; i < endIndex; i++) {
          if (i > startIndex) {
              buf.append(separator);
          }
          if (array[i] != null) {
              buf.append(array[i]);
          }
      }
      return buf.toString();
  }
  
  public static String[] splitPreserveAllTokens(String str, char separatorChar) {
      return splitWorker(str, separatorChar, true);
  }
  
  private static String[] splitWorker(String str, char separatorChar, boolean preserveAllTokens) {
      // Performance tuned for 2.0 (JDK1.4)

      if (str == null) {
          return null;
      }
      int len = str.length();
      if (len == 0) {
          return new String[0];
      }
      List list = new ArrayList();
      int i = 0, start = 0;
      boolean match = false;
      boolean lastMatch = false;
      while (i < len) {
          if (str.charAt(i) == separatorChar) {
              if (match || preserveAllTokens) {
                  list.add(str.substring(start, i));
                  match = false;
                  lastMatch = true;
              }
              start = ++i;
              continue;
          }
          lastMatch = false;
          match = true;
          i++;
      }
      if (match || (preserveAllTokens && lastMatch)) {
          list.add(str.substring(start, i));
      }
      return (String[]) list.toArray(new String[list.size()]);
  }
  
  public static String strip(String str, String stripChars) {
      if (isEmpty(str)) {
          return str;
      }
      str = stripStart(str, stripChars);
      return stripEnd(str, stripChars);
  }
  
  public static String stripStart(String str, String stripChars) {
      int strLen;
      if (str == null || (strLen = str.length()) == 0) {
          return str;
      }
      int start = 0;
      if (stripChars == null) {
          while ((start != strLen) && Character.isWhitespace(str.charAt(start))) {
              start++;
          }
      } else if (stripChars.length() == 0) {
          return str;
      } else {
          while ((start != strLen) && (stripChars.indexOf(str.charAt(start)) != -1)) {
              start++;
          }
      }
      return str.substring(start);
  }
  
  public static String stripEnd(String str, String stripChars) {
      int end;
      if (str == null || (end = str.length()) == 0) {
          return str;
      }

      if (stripChars == null) {
          while ((end != 0) && Character.isWhitespace(str.charAt(end - 1))) {
              end--;
          }
      } else if (stripChars.length() == 0) {
          return str;
      } else {
          while ((end != 0) && (stripChars.indexOf(str.charAt(end - 1)) != -1)) {
              end--;
          }
      }
      return str.substring(0, end);
  }
  
  public static boolean equals(String str1, String str2) {
      return Objects.equals(str1, str2);
  }

  public static boolean equalsIgnoreCase(String s1, String s2) {
    return s1 == null ? s2 == null : s1.equalsIgnoreCase(s2);
  }

  public static boolean equalsIgnoreCase(List<String> l1, List<String> l2) {
    if (l1 == l2) {
      return true;
    }

    if (l1 == null || l2 == null || l1.size() != l2.size()) {
      return false;
    }

    for (int i = 0; i < l1.size(); i++) {
      if (!equalsIgnoreCase(l1.get(i), l2.get(i))) {
        return false;
      }
    }

    return true;
  }

  public static List<String> toLowerCase(List<String> names)
  {
    return names.stream().map(String::toLowerCase).collect(Collectors.toList());
  }

  public static final byte[] encodeQuotedPrintable(final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    for (final byte c : bytes) {
      int b = c;
      if (b < 0) {
        b = 256 + b;
      }
      if (PRINTABLE_CHARS.get(b)) {
        buffer.write(b);
      } else {
        buffer.write(PRINTABLE_ESCAPE_CHAR);
        final char hex1 = Character.toUpperCase(Character.forDigit((b >> 4) & 0xF, 16));
        final char hex2 = Character.toUpperCase(Character.forDigit(b & 0xF, 16));
        buffer.write(hex1);
        buffer.write(hex2);
      }
    }

    return buffer.toByteArray();
  }
}
