package com.aliyun.odps.data;

import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhenhong.gzh on 16/12/14.
 */
public class VarCharTest {
  @Test
  public void test() throws UnsupportedEncodingException {
    String str = "hello world";
    Varchar varchar = new Varchar(str);

    Assert.assertEquals(varchar, new Varchar(new String("hello world")));
    Assert.assertEquals(varchar.getValue(), str);
    Assert.assertEquals(varchar.length(), str.length());
    Assert.assertEquals(varchar.toString(), str.toString());

    varchar = new Varchar(str, 2);
    Assert.assertEquals(varchar, new Varchar(new String(str.substring(0, 2))));
    Assert.assertEquals(varchar.getValue(), "he");
    Assert.assertEquals(varchar.length(), 2);
    Assert.assertEquals(varchar.toString(), "he");
  }
}
