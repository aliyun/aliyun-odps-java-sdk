package com.aliyun.odps;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SessionQueryResult;
import org.junit.*;

import java.util.*;

@Ignore
public class SessionV2Test extends TestBase {
  private static Session session = null;
  private static String sql = "select COUNT(*) from src;";
  private static String sessionName = "test_sdk_session" + System.currentTimeMillis();
  private static String sessionId = null;
  private static List<String> sqlRes;

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

    session = createSession(sessionName);

    SessionQueryResult result = session.run(sql);
    sqlRes = printResult(result.getRecordIterator());
    Assert.assertEquals(sqlRes.size(), 1);
  }

  public static Session createSession(String name) throws OdpsException {
    Map<String, String> flags = new HashMap<String, String>();
    flags.put("odps.sql.session.worker.count", "5");
    flags.put("odps.sql.session.worker.sparespan", "0-0");
    flags.put("odps.sql.session.enable.start.service", "true");
    flags.put("odps.sql.session.split.cache.by.page", "false");
    flags.put("odps.sql.jobconf.odps2", "true");
    flags.put("odps.sql.submit.mode", "true");
    flags.put("odps.sql.session.worker.memory", "2048");
    flags.put("odps.optimizer.split", "false");
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

    Iterator<Record> res = copySession.run(sql, hints).getRecordIterator();

    List<String> result = printResult(res);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testGetSqlStats() throws OdpsException {
    Session copySession = new Session(odps, session.getInstance());

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.session.wait.finished.status", "true");

    Iterator<Record> res = copySession.run(sql, hints).getRecordIterator();

    List<String> result = printResult(res);
    Assert.assertEquals(result.size(), 1);

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

    Iterator<Record> res = attachSession.run(sql).getRecordIterator();

    List<String> result = printResult(res);
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testListSession() throws OdpsException {
    Sessions sessions = new Sessions(odps);

    Iterator<Session.SessionItem> itemIterator = sessions.iterator();
    while (itemIterator.hasNext()) {
      Session.SessionItem item = itemIterator.next();
      Assert.assertEquals(item.sessionId, sessionId);
      Assert.assertEquals(item.aliasName, sessionName);
      //System.out.println("111" + item.sessionId);
    }
    Iterable<Session.SessionItem> itemIterable = sessions.iterable();
    for (Session.SessionItem item : itemIterable) {
      Assert.assertEquals(item.sessionId, sessionId);
      Assert.assertEquals(item.aliasName, sessionName);
      //System.out.println("222" + item.sessionId);
    }
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
