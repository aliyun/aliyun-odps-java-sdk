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

package com.aliyun.odps.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * 守卫（the original regression）：本模块里 JUnit 4 写法的用例等于没写。
 *
 * <p>odps-sdk-storage-api 的 surefire 3.2.5 只挂 junit-platform provider，测试依赖里没有
 * junit-vintage-engine，而根 pom 又给每个模块都塞了 test 作用域的 junit:junit:4.13.1 ——
 * 所以 {@code org.junit.Test} 写的类能编译、构建照样绿，但一条都不会执行。2026-09-30 在
 * develop 上实测：模块 18 个测试源文件、95 个 {@code @Test}，surefire 只报了 16 个类、83 条，
 * OkHttpProxyHelperTest 与 TableReadSessionTest 连报告文件都没有生成。
 *
 * <p>这条用例不是再测一遍功能，是把那扇门关上：只要 {@code src/test/java} 下还有 JUnit 4 的
 * import，构建就显式失败并点名文件与行号，而不是静默少跑几条。找不到测试源码根目录同样直接失败 ——
 * "找不到就跳过"正是本守卫要防的行为。
 */
class JUnit4SyntaxGuardTest {

  /**
   * {@code org.junit.} 后面不接 jupiter / platform / vintage 的，就是 JUnit 4：
   * JUnit 5 的东西全在 {@code org.junit.jupiter.} 与 {@code org.junit.platform.} 底下。
   */
  private static final Pattern JUNIT4_IMPORT = Pattern.compile(
      "^\\s*import\\s+(static\\s+)?org\\.junit\\.(?!jupiter\\.|platform\\.|vintage\\.)[A-Za-z_*]");

  private static final String MODULE_PATH = "odps-sdk/odps-sdk-storage-api";

  private static final String SELF = "JUnit4SyntaxGuardTest.java";

  @Test
  void wildcardJUnit4ImportIsRejected() {
    assertTrue(JUNIT4_IMPORT.matcher("import org.junit.*;").find());
    assertFalse(JUNIT4_IMPORT.matcher("import org.junit.jupiter.api.*;").find());
  }

  @Test
  void noJUnit4ImportsUnderTestSources() throws IOException {
    Path testRoot = locateTestSourceRoot();
    List<Path> javaFiles = new ArrayList<>();
    try (Stream<Path> walk = Files.walk(testRoot)) {
      walk.filter(path -> path.getFileName().toString().endsWith(".java"))
          .sorted()
          .forEach(javaFiles::add);
    }

    // 自我核对：扫到的文件里必须有本守卫自己，否则说明根目录找错了，"没有违例"就成了假绿。
    boolean scannedSelf = false;
    List<String> offenders = new ArrayList<>();
    for (Path file : javaFiles) {
      if (SELF.equals(file.getFileName().toString())) {
        scannedSelf = true;
      }
      int lineNo = 0;
      for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
        lineNo++;
        if (JUNIT4_IMPORT.matcher(line).find()) {
          offenders.add(testRoot.relativize(file) + ":" + lineNo + ": " + line.trim());
        }
      }
    }

    assertTrue(scannedSelf, "在 " + testRoot + " 下没扫到 " + SELF + "，扫描范围可疑，不按"
        + "「零违例」放行。");

    if (!offenders.isEmpty()) {
      fail("发现 " + offenders.size() + " 处 JUnit 4 import。本模块的 surefire 没有 "
          + "junit-vintage-engine，这些用例不会被执行，只会把零覆盖伪装成绿色"
          + "（the original regression）。请改成 org.junit.jupiter.api.* 再提交：\n  "
          + String.join("\n  ", offenders)
          + "\n扫描根：" + testRoot + "，共 " + javaFiles.size() + " 个 .java");
    }
  }

  /**
   * 上一条守卫的前提是「没有 vintage engine，所以 JUnit 4 不会被执行」。哪天有人把
   * junit-vintage-engine 装进测试依赖，前提就变了：JUnit 4 用例会真的开始跑，那是一次框架
   * 口径变更，应该由人显式处理两条守卫，而不是悄悄装完继续两套并存。
   */
  @Test
  void vintageEngineStaysOffTheTestClasspath() {
    boolean present;
    try {
      Class.forName("org.junit.vintage.engine.VintageTestEngine", false,
          getClass().getClassLoader());
      present = true;
    } catch (ClassNotFoundException expected) {
      present = false;
    }

    assertFalse(present,
        "测试 classpath 上出现了 junit-vintage-engine：JUnit 4 用例从此会真的执行，"
            + "上一条守卫「写了也不跑」的理由不再成立。本模块的口径仍是单一框架 JUnit 5 —— "
            + "要么把剩下的 JUnit 4 用例迁完再撤掉这两条守卫，要么在 the original regression 上说清为什么要"
            + "改回两套并存；不要悄悄装了就走。");
  }

  /**
   * surefire 的工作目录是模块 basedir，但 IDE 与手工运行可能是仓库根或别的目录：
   * 依次试 basedir、user.dir、maven.multiModuleProjectDirectory，再向上找模块路径。
   * 全都找不到就 fail —— 静默放行等于把本守卫变成摆设。
   */
  private static Path locateTestSourceRoot() {
    Set<Path> bases = new LinkedHashSet<>();
    addDirectory(bases, System.getProperty("basedir"));
    addDirectory(bases, System.getProperty("user.dir"));
    String multiModule = System.getProperty("maven.multiModuleProjectDirectory");
    if (multiModule != null && !multiModule.isEmpty()) {
      bases.add(Paths.get(multiModule, MODULE_PATH).toAbsolutePath().normalize());
    }
    Path cursor = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
    for (int depth = 0; depth < 6 && cursor != null; depth++) {
      bases.add(cursor);
      bases.add(cursor.resolve(MODULE_PATH));
      cursor = cursor.getParent();
    }

    for (Path base : bases) {
      Path candidate = base.resolve(Paths.get("src", "test", "java"));
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
    }

    fail("找不到本模块的 src/test/java，试过的候选：" + bases + "。请在 Maven 构建里运行本用例"
        + "（mvn -pl odps-sdk/odps-sdk-storage-api test）。这条守卫宁可把构建弄红，也不会在"
        + "看不见源码的时候假装通过 —— 那正是 87352108 的老毛病。");
    return null;
  }

  private static void addDirectory(Set<Path> set, String raw) {
    if (raw == null || raw.isEmpty()) {
      return;
    }
    Path path = Paths.get(raw).toAbsolutePath().normalize();
    if (Files.isDirectory(path)) {
      set.add(path);
    }
  }
}
