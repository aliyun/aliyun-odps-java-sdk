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

package com.aliyun.odps.sqa.commandapi.exception;

import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;

/**
 * 自定义ANTLR的错误处理机制 避免无意义的错误恢复
 */
public class CommandErrorStrategy extends DefaultErrorStrategy {

  /**
   * 不从异常e中恢复, 而是用RuntimeException包装,这样就不会被规则函数的catch捕获
   *
   * @param recognizer
   * @param e
   */
  @Override
  public void recover(Parser recognizer, RecognitionException e) {
    throw new RuntimeException("command does not meet the semantics");
  }

  /**
   * 确保不会试图执行行内恢复, 如果语法分析器成功进行了恢复,就不会抛出一个异常
   *
   * @param recognizer
   * @return
   * @throws RecognitionException
   */
  @Override
  public Token recoverInline(Parser recognizer) throws RecognitionException {
    throw new RuntimeException(new InputMismatchException(recognizer));
  }

  /**
   * 确保不会试图从子规则的问题中恢复
   *
   * @param recognizer
   */
  @Override
  public void sync(Parser recognizer) {
  }

  /**
   * 修改语法分析器的错误报告策略"line xx:xx no viable alternative at input"
   *
   * @param recognizer
   * @param e
   */
  @Override
  protected void reportNoViableAlternative(Parser recognizer, NoViableAltException e) {

  }
}
