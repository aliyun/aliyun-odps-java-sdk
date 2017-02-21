package com.aliyun.odps.data;

import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhenhong.gzh on 16/12/14.
 */
public class BinaryTest {
  @Test
  public void test() throws UnsupportedEncodingException {
    byte [] bytes = new byte [] {-54, -10, -120, 66, 102, -64, 57, -94};

    Binary binary = new Binary(bytes);

    Assert.assertEquals(binary, new Binary(bytes));
    Assert.assertArrayEquals(binary.data(), bytes);
    Assert.assertEquals(binary.length(), bytes.length);
    Assert.assertEquals("=CA=F6=88Bf=C09=A2", binary.toString());
  }
}
