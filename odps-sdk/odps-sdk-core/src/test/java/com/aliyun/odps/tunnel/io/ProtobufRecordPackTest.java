package com.aliyun.odps.tunnel.io;

import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;

/**
 * Created by ruibo.lirb on 2015-11-24.
 */
public class ProtobufRecordPackTest {

  @Test
  public void testComplete() throws IOException {
    TableSchema s = new TableSchema();
    s.addColumn(new Column("i", OdpsType.BIGINT));
    ArrayRecord r = new ArrayRecord(s);
    r.setBigint(0, 1L);

    ProtobufRecordPack p = new ProtobufRecordPack(s);
    p.append(r);
    Assert.assertEquals(p.getSize(), 1L);

    p.complete();
    long b1 = p.getTotalBytes();
    Assert.assertNotEquals(b1, 0L);
    Assert.assertTrue(p.isComplete());

    p.complete(); // complete again should do nothing
    long b2 = p.getTotalBytes();
    Assert.assertEquals(b1, b2);

    p.reset();
    Assert.assertFalse(p.isComplete());
    Assert.assertEquals(p.getSize(), 0L);
    Assert.assertEquals(p.getTotalBytes(), 0L);
  }

}
