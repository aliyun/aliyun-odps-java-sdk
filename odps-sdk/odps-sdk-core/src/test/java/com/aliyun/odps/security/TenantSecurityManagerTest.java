package com.aliyun.odps.security;

import java.lang.reflect.Type;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TenantSecurityManager;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.security.SecurityManager.AuthorizationQueryInstance;
import com.aliyun.odps.utils.GsonObjectBuilder;
import com.google.gson.reflect.TypeToken;

public class TenantSecurityManagerTest {

  private static String TENANT_ROLE_PREFIX = "TenantSecurityManagerTest_";

  private static String POLICY = "{\n"
      + "    \"Statement\": [{\n"
      + "            \"Action\": [\"odps:CreateQuota\",\n"
      + "                \"odps:Usage\"],\n"
      + "            \"Effect\": \"Allow\",\n"
      + "            \"Resource\": [\"acs:odps:*:quotas/*\"]},\n"
      + "        {\n"
      + "            \"Action\": [\"odps:CreateNetworkLink\",\n"
      + "                \"odps:Usage\"],\n"
      + "            \"Effect\": \"Allow\",\n"
      + "            \"Resource\": [\"acs:odps:*:networklinks/*\"]},\n"
      + "        {\n"
      + "            \"Action\": [\"odps:*\"],\n"
      + "            \"Effect\": \"Allow\",\n"
      + "            \"Resource\": [\"acs:odps:*:projects/*\"]}],\n"
      + "    \"Version\": \"1\"}";

  private static String createTenantRole() throws OdpsException {
    String tenantRoleName = OdpsTestUtils.getRandomName(TENANT_ROLE_PREFIX);
    Odps odps = OdpsTestUtils.newDefaultOdps();
    AuthorizationQueryInstance i = odps
        .projects()
        .get()
        .getSecurityManager()
        .run("CREATE TENANT ROLE " + tenantRoleName, false);
    i.waitForSuccess();

    return tenantRoleName;
  }

  @AfterClass
  public static void tearDown() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    SecurityManager sm = odps.projects().get().getSecurityManager();
    AuthorizationQueryInstance i = sm.run("LIST TENANT ROLES",true);
    i.waitForSuccess();

    Type t = new TypeToken<List<String>>(){}.getType();
    List<String> tenantRoles = GsonObjectBuilder.get().fromJson(i.getResult(), t);
    for (String tenantRole : tenantRoles) {
      if (tenantRole.startsWith(TENANT_ROLE_PREFIX)) {
        String query = "DROP TENANT ROLE " + tenantRole;
        try {
          sm.runQuery(query, false);
          System.err.println("Tenant role " + tenantRole + " has been dropped");
        } catch (OdpsException e) {
          System.err.println(
              "Exception happened when running: " + query + ", message: " + e.getMessage());
        }
      }
    }
  }

  @Test
  public void testPutAndGetPolicy() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    TenantSecurityManager tsm = odps.tenant().getTenantSecurityManager();

    String tenantRole = createTenantRole();
    tsm.putTenantRolePolicy(tenantRole, POLICY);

    String policy = tsm.getTenantRolePolicy(tenantRole);
    Assert.assertEquals(POLICY, policy);
  }
}
