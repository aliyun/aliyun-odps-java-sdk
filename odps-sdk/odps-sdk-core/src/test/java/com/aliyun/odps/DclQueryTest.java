package com.aliyun.odps;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;
import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DclQueryTest extends TestBase {
  HashMap<String, String> hints = new LinkedHashMap<>();
  HashMap<String, String> settings = new LinkedHashMap<>();
  String grantUser;

  @Before
  public void setUp() throws Exception {
    hints.put("odps.auth.query.format", "json");
    settings.put("odps.auth.query.format", "json");
    settings.put("odps.sql.sqltask.output.planonly", "true");

    odps = OdpsTestUtils.newDefaultOdps();
    grantUser = OdpsTestUtils.getGrantUser();
    if (!grantUser.toUpperCase().startsWith("ALIYUN$")) {
      grantUser = "ALIYUN$" + grantUser;
    }
    try {
      Instance inst = SQLTask.run(odps, odps.getDefaultProject(), "add user " + grantUser + ";", hints, null);
      inst.waitForSuccess();
    } catch (OdpsException e) {
    }
    try {
      Instance inst = SQLTask.run(odps, odps.getDefaultProject(), "grant admin to " + grantUser + ";", hints, null);
      inst.waitForSuccess();
    } catch (OdpsException e) {
    }
  }

  @Test
  public void testShowGrants() throws OdpsException {
    Instance inst = SQLTask.run(odps, odps.getDefaultProject(), "show grants on table no_exist;", hints, null);
    assertFalse(inst.isSync());
  }

  @Test
  public void testShowGrantsPlanOnly() throws OdpsException {
    Instance inst = SQLTask.run(odps, odps.getDefaultProject(), "show grants on table no_exist;", settings, null);
    assertTrue(inst.isSync());
  }

  @Test
  public void testShowGrantsForUser() throws OdpsException {
    Instance inst = SQLTask.run(odps, odps.getDefaultProject(), "show grants for user " + grantUser + ";", hints, null);
    assertFalse(inst.isSync());
    inst.waitForSuccess();
    for (String key : inst.getTaskResultsWithFormat().keySet()) {
      String instString = inst.getTaskResultsWithFormat().get(key).getString();
      Gson gson = new Gson();
      HashMap<String, String> instMap = gson.fromJson(instString, HashMap.class);
      String resultString = instMap.get("result");
      HashMap<?,?> resultMap = gson.fromJson(resultString, HashMap.class);
      Object roles = resultMap.get("Roles");
      if (roles instanceof List) {
        List<String> roleList = (List<String>) roles;
        assertTrue(roleList.contains("admin"));
      }
    }
  }

  @Test
  public void testListUsers() throws OdpsException {
    Instance inst = SQLTask.run(odps, odps.getDefaultProject(), "list users;", hints, null);
    assertFalse(inst.isSync());
  }

  @Test
  public void testListRowAccessPolicy() throws OdpsException {
    Instance inst = SQLTask.run(odps, odps.getDefaultProject(), "list row access policy on not_exist;", hints, null);
    assertTrue(inst.isSync());
  }
}
