package com.aliyun.odps;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.data.SessionQueryResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

public class SessionV2Test extends TestBase {
  private static Session session = null;
  private static String sessionName = "test_sdk_session" + System.currentTimeMillis();
  private static String tableName = "test_session_table" + System.currentTimeMillis();
  private static String sessionId = null;
  private static String sql = "select COUNT(*) from " + tableName + ";";
  private static String sqlColNotExistException = "select colNotExist from " + tableName + ";";
  private static String sqlTableNotFoundException = "select * from tableNameNotExist;";

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
    flags.put("odps.sql.session.slot.worker.ratio", "0.6");
    flags.put("odps.sql.session.isolation.worker.ratio", "0.2");
    flags.put("odps.sql.session.start.timeout", "1200000");

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
  public void testGetSessionExceptionCodeStats() throws OdpsException {
    Session copySession = new Session(odps, session.getInstance());

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.session.wait.finished.status", "true");

    String exceptionValue = new String();
    Session.SubQueryInfo info = copySession.runSubQuery(sqlColNotExistException, hints);
    try {
      copySession.getSubQueryResult(info.queryId).getRecords();
    }
    catch (OdpsException e)
    {
      exceptionValue = e.toString();
      Assert.assertTrue(exceptionValue.startsWith("com.aliyun.odps.OdpsException: ODPS-0130071"));
    }
    System.out.println(exceptionValue);

    info = copySession.runSubQuery(sqlTableNotFoundException, hints);
    try {
      copySession.getSubQueryResult(info.queryId).getRecords();
    }
    catch (OdpsException e)
    {
      exceptionValue = e.toString();
      Assert.assertTrue(exceptionValue.startsWith("com.aliyun.odps.OdpsException: ODPS-0130131"));
    }

    System.out.println(exceptionValue);
  }

  @Test
  @Ignore
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

    String list = copySession.getInformation("get_blacklist");
    System.out.println(list);
    copySession.setInformation("add_blacklist", "test_project");
    copySession.setInformation("add_blacklist", odps.getDefaultProject());

    list = copySession.getInformation("get_blacklist");
    Assert.assertTrue(list.contains(odps.getDefaultProject()));
    Assert.assertTrue(list.contains("test_project"));

    copySession.setInformation("remove_blacklist", odps.getDefaultProject());
    list = copySession.getInformation("get_blacklist");
    Assert.assertFalse(list.contains(odps.getDefaultProject()));
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