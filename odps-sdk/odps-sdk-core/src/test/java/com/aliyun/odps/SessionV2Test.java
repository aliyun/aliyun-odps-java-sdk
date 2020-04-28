package com.aliyun.odps;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.data.SessionQueryResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class SessionV2Test extends TestBase {
  private static Session session = null;
  private static String sessionName = "test_sdk_session" + System.currentTimeMillis();
  private static String tableName = "test_session_table" + System.currentTimeMillis();
  private static String sessionId = null;
  private static String sql = "select COUNT(*) from " + tableName + ";";
  private static String cacheOnA = "@a := CACHE ON SELECT `COUNT`(*) FROM " + tableName + ";";
  private static String cacheOnB = "@b := CACHE ON SELECT * FROM " + tableName + ";";
  private static Map<String, String> emptyHint = new HashMap();
  @AfterClass
  public static void after() throws OdpsException {
    odps.tables().delete(tableName, true);
    if (session != null) {
      session.stop();
    }
  }

  @BeforeClass
  public static void testCreateSession() throws OdpsException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.STRING));
    schema.addColumn(new Column("c2", OdpsType.BIGINT));

    odps.tables().create(tableName, schema, true);

    session = createSession(sessionName);

    Session.SubQueryInfo info = session.runSubQuery(sql, emptyHint);
    checkResult(session.getSubQueryResult(info.queryId).getRecords());
  }

  public static Session createSession(String name) throws OdpsException {
    Map<String, String> flags = new HashMap<String, String>();
    flags.put("odps.sql.session.worker.count", "5");
    flags.put("odps.sql.session.worker.sparespan", "0-0");
    flags.put("odps.sql.jobconf.odps2", "true");
    flags.put("odps.sql.session.worker.memory", "2048");
    flags.put("odps.sql.session.version2", "true");
    flags.put("odps.sql.session.worker.cpu", "33");
    flags.put("odps.sql.session.worker.cache.size", "64");
    flags.put("set odps.sql.session.max.slot.number", "5");

    session = Session.create(odps, name, odps.getDefaultProject(), flags, 0L);
    System.out.println("Create session success: " + session.getInstance().getId());
    Instance i = session.getInstance();
    System.out.println(odps.logview().generateLogView(i, 7*24));
    session.waitForStart(0);
    sessionId = session.getInstance().getId();
    return session;
  }

  @Test
  public void testUseSession() throws OdpsException {
    Session copySession = new Session(odps, session.getInstance());

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.session.wait.finished.status", "true");

    Session.SubQueryInfo info = copySession.runSubQuery(sql, hints);
    checkResult(copySession.getSubQueryResult(info.queryId).getRecords());
  }

  //@Test
  public void testGetSqlStats() throws OdpsException {
    Session copySession = new Session(odps, session.getInstance());

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.session.wait.finished.status", "true");

    Session.SubQueryInfo info = copySession.runSubQuery(sql, hints);
    checkResult(copySession.getSubQueryResult(info.queryId).getRecords());

    String stats = copySession.getQueryStats();
    Assert.assertTrue(stats.startsWith("resource cost"));
    System.out.println(stats);
  }

  @Test
  public void testAttachSession() throws OdpsException {
    Session attachSession = Session.attach(odps, sessionName);
    System.out.println("Attatch session success: " + session.getInstance().getId());
    Instance i = session.getInstance();
    System.out.println(odps.logview().generateLogView(i, 7*24));
    attachSession.waitForStart();

    Session.SubQueryInfo info = attachSession.runSubQuery(sql, emptyHint);
    checkResult(attachSession.getSubQueryResult(info.queryId).getRecords());
  }

  @Test
  public void testListSession() throws OdpsException {
    Sessions sessions = new Sessions(odps);
    boolean found = false;
    Iterator<Session.SessionItem> itemIterator = sessions.iterator();
    while (itemIterator.hasNext()) {
      Session.SessionItem item = itemIterator.next();
      if (item.sessionId.equals(sessionId) && item.aliasName.equals(sessionName)) {
        found = true;
      }
    }
    Assert.assertTrue(found);
    found = false;
    Iterable<Session.SessionItem> itemIterable = sessions.iterable();
    for (Session.SessionItem item : itemIterable) {
      if (item.sessionId.equals(sessionId) && item.aliasName.equals(sessionName)) {
        found = true;
      }
    }
    Assert.assertTrue(found);
  }

  @Test
  public void testSetGetInformation() throws OdpsException {
    Session copySession = new Session(odps, session.getInstance());

    String whitelist = copySession.getInformation("get_whitelist");

    Assert.assertTrue(whitelist.contains(odps.getDefaultProject()));
    System.out.println(whitelist);
    copySession.setInformation("add_whitelist", "test_project");

    whitelist = copySession.getInformation("get_whitelist");
    Assert.assertTrue(whitelist.contains(odps.getDefaultProject()));
    Assert.assertTrue(whitelist.contains("test_project"));
  }

  @Test
  public void testVariables() throws OdpsException {
    Session attachSession = Session.attach(odps, sessionName);
    System.out.println("Attatch session success: " + session.getInstance().getId());
    Instance i = session.getInstance();
    System.out.println(odps.logview().generateLogView(i, 7*24));
    attachSession.waitForStart();

    Map<String, String> hints = new HashMap();
    hints.put("odps.sql.session.select.only", "false");
    List<String> vars = attachSession.showVariables(hints);
    Assert.assertEquals(vars.size(), 0);
    Session.SubQueryInfo subQueryInfo1 = attachSession.runSubQuery(cacheOnA, hints);
    Session.SubQueryResult result1 = attachSession.getSubQueryResult(subQueryInfo1.queryId);
    Assert.assertEquals(result1.getRecords().size(), 0);
    Session.SubQueryInfo subQueryInfo2 = attachSession.runSubQuery(cacheOnB, hints);
    Session.SubQueryResult result2 = attachSession.getSubQueryResult(subQueryInfo2.queryId);
    Assert.assertEquals(result2.getRecords().size(), 0);
    vars = attachSession.showVariables(hints);
    Assert.assertEquals(vars.size(), 2);
    Assert.assertEquals(vars.get(0), cacheOnA);
    Assert.assertEquals(vars.get(1), cacheOnB);
  }

  private static void checkResult(List<Record> records) {
    Assert.assertEquals(records.size(), 1);
    for (Record r : records){
      System.out.print("Get new record: ");
      for (int k = 0; k < r.getColumnCount(); k++) {
        System.out.print("\tColumn index: " + k);
        System.out.print("\t value: " + r.getString(k));
      }
    }
  }

}
