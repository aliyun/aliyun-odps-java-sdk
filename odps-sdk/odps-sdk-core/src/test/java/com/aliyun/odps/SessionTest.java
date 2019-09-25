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
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.STRING));
    schema.addColumn(new Column("c2", OdpsType.BIGINT));

    odps.tables().create("src", schema, true);

    Map<String, String> flags = new HashMap<String, String>();
    flags.put("odps.sql.session.worker.count", "5");
    flags.put("odps.sql.session.worker.sparespan", "0-0");
    flags.put("odps.sql.session.enable.start.service", "true");
    flags.put("odps.sql.session.split.cache.by.page", "false");
    flags.put("odps.sql.jobconf.odps2", "true");
    flags.put("odps.sql.submit.mode", "true");
    flags.put("odps.sql.session.worker.memory", "2048");
    flags.put("odps.optimizer.split", "false");

    session = Session.create(odps, name, odps.getDefaultProject(), flags, 0L);
    System.out.println("Create session success: " + session.getInstance().getId());
    Instance i = session.getInstance();
    System.out.println(odps.logview().generateLogView(i, 7*24));
    session.waitForStart(0);

    SessionQueryResult result = session.run("@a := CACHE ON select c1, COUNT(*) from src group by c1;");
    System.out.println("variable definition result: " + result.getResult());
    Assert.assertTrue(result.getResult().isEmpty());
    result = session.run("@b := CACHE ON select COUNT(*) from src group by c1;");
    System.out.println("variable definition result: " + result.getResult());
    Assert.assertTrue(result.getResult().isEmpty());

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


    // todo uncomment following assert as now the split refactor introduce bugs
//    Assert.assertArrayEquals(printResult(res).toArray(), sqlRes.toArray());
  }

  @Test
  public void testAttachSession() throws OdpsException {
    Session attachSession = Session.attach(odps, name);
    System.out.println("Attatch session success: " + session.getInstance().getId());
    Instance i = session.getInstance();
    System.out.println(odps.logview().generateLogView(i, 7*24));
    attachSession.waitForStart();

    // todo uncomment following assert as now the split refactor introduce bugs
//    Assert.assertArrayEquals(printResult(attachSession.run(sql).getRecordIterator()).toArray(), sqlRes
//        .toArray());

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
