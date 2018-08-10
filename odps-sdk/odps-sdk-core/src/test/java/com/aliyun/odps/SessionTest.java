package com.aliyun.odps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SessionQueryResult;

public class SessionTest extends TestBase{
  private static Session session = null;
  private static String sql = "select * from @a;";
  private static String name = "test_sdk_session" + System.currentTimeMillis();
  private static List<String> sqlRes;
  private static String variables;

  @AfterClass
  public static void after() throws OdpsException {
    if (session != null) {
      session.stop();
    }
  }

  @BeforeClass
  public static void testCreateSession() throws OdpsException {
    session = Session.create(odps, 3, 20, name, odps.getDefaultProject(), null, null, null);
    System.out.println("Create session success: " + session.getInstance().getId());
    Instance i = session.getInstance();
    System.out.println(odps.logview().generateLogView(i, 7*24));
    session.waitForStart(0);

    SessionQueryResult result = session.run("@a := CACHE ON select c1, COUNT(*) from src group by c1;");
    Assert.assertFalse(result.getResultIterator().hasNext());
    result = session.run("@b := CACHE ON select COUNT(*) from src group by c1");
    Assert.assertFalse(result.getResultIterator().hasNext());

    result = session.run(sql);
    sqlRes = printResult(result.getRecordIterator());
    //
    variables = session.run("show variables;").getResult();
    System.out.println(variables);

    Assert.assertTrue(variables.contains("@a"));
    Assert.assertTrue(variables.contains("@b"));

  }

  @Test
  public void testUseSession() throws OdpsException {
    Session copySession = new Session(odps, session.getInstance());

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.session.wait.finished.status", "true");

    Iterator<Record> res = copySession.run(sql, hints).getRecordIterator();


    Assert.assertArrayEquals(printResult(res).toArray(), sqlRes.toArray());
  }

  @Test
  public void testAttachSession() throws OdpsException {
    Session attachSession = Session.attach(odps, name);
    System.out.println("Attatch session success: " + session.getInstance().getId());
    Instance i = session.getInstance();
    System.out.println(odps.logview().generateLogView(i, 7*24));
    attachSession.waitForStart();

    Assert.assertArrayEquals(printResult(attachSession.run(sql).getRecordIterator()).toArray(), sqlRes
        .toArray());

    String vars = attachSession.run("show variables;").getResult();
    Assert.assertEquals(variables, vars);
  }

  private static List<String> printResult(Iterator<Record> res) {
    List<String> result = null;

    while(res.hasNext()) {
      result = new ArrayList<String>();
      System.out.print("Get new record: ");
      Record r = res.next();
      for (int k = 0; k < r.getColumnCount(); k++) {
        System.out.print("\tColumn index: " + k);
        System.out.print("\t value: " + r.getString(k));
        result.add(r.getString(k));
      }
      System.out.println();
    }

    return result;
  }

}
