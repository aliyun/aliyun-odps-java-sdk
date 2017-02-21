package com.aliyun.odps.data;

import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhenhong.gzh on 16/12/14.
 */
public class CharTest {
  @Test
  public void test() throws UnsupportedEncodingException {
    String str = "hello world";
    Char chars = new Char(str);

    Assert.assertEquals(chars, new Char(new String("hello world")));
    Assert.assertEquals(chars.getValue(), str);
    Assert.assertEquals(chars.length(), str.length());
    Assert.assertEquals(chars.toString(), str.toString());

    chars = new Char(str, 2);
    Assert.assertEquals(chars, new Char(new String(str.substring(0, 2))));
    Assert.assertEquals(chars.getValue(), "he");
    Assert.assertEquals(chars.length(), 2);
    Assert.assertEquals(chars.toString(), "he");
  }
}
