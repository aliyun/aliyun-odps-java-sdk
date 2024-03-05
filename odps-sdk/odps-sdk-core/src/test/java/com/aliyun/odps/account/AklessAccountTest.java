package com.aliyun.odps.account;

import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.auth.credentials.ICredential;
import com.aliyun.auth.credentials.provider.ICredentialProvider;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.security.SecurityManager;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class AklessAccountTest {

  /**
   * need credprovider.plugin dependency
   */
  @Test
  @Ignore("only support in public/office region")
  public void testInit() throws OdpsException {
    String ramRole = OdpsTestUtils.getRamRole();
    System.out.println(ramRole);
//    CredentialProviderConfig credentialProviderConfig =
//        new CredentialProviderConfig(ramRole, "office", "odps-sdk-java", "", "testing", 60L);
    ICredentialProvider credentialsProvider = null;
    ICredential credentials = credentialsProvider.getCredentials();
    System.out.println(credentials.accessKeyId());
    System.out.println(credentials.accessKeySecret());
    System.out.println(credentials.securityToken());

    Account account = new AklessAccount(credentialsProvider);

    Odps odps = new Odps(account);
    odps.setEndpoint("http://service.cn-zhangjiakou.maxcompute.aliyun.com/api");
    SecurityManager manager = odps.projects().get("akless").getSecurityManager();
    SecurityManager.AuthorizationQueryInstance whoami = manager.run("whoami", false);
    System.out.println(whoami.getResult());
  }
}
