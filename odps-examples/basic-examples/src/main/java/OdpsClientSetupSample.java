/**
 * OdpsClientSetupSample.java
 *
 * This example class demonstrates how to construct Alibaba Cloud MaxCompute (formerly ODPS) client instances based on various authentication methods.
 * MaxCompute is a large-scale data processing and analytics service; this SDK facilitates rapid integration and usage of the service.
 */

import com.aliyun.auth.credentials.provider.ICredentialProvider;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AklessAccount;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.AppAccount;
import com.aliyun.odps.account.BearerTokenAccount;
import com.aliyun.odps.account.StsAccount;

/**
 * OdpsClientSetupSample provides static methods for constructing Odps clients,
 * supporting authentication via AccessKey, STS Token, Aliyun Credential Provider, Dual-Signature Authentication, and Bearer Token.
 */
public class OdpsClientSetupSample {
  // Replace this Endpoint with your actual MaxCompute service address
  private static final String SAMPLE_ENDPOINT = "<your odps endpoint>";

  /**
   * Constructs an Odps client using AccessKey.
   * Requires users to provide an AccessId and AccessKey.
   * <a href="https://help.aliyun.com/zh/ram/user-guide/create-an-accesskey-pair">How to create and obtain AccessKey</a>
   *
   * @param accessId  User's AccessId.
   * @param accessKey User's AccessKey.
   * @return An initialized Odps instance.
   */
  public static Odps buildWithAccessKey(String accessId, String accessKey) {
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);
    odps.setEndpoint(SAMPLE_ENDPOINT);
    return odps;
  }

  /**
   * Constructs an Odps client using STS Token.
   * Suitable for temporary authorization scenarios.
   *
   * @param accessId  User's AccessId.
   * @param accessKey User's AccessKey.
   * @param stsToken  The STS Token.
   * @return An initialized Odps instance.
   */
  public static Odps buildWithStsToken(String accessId, String accessKey, String stsToken) {
    Account account = new StsAccount(accessId, accessKey, stsToken);
    Odps odps = new Odps(account);
    odps.setEndpoint(SAMPLE_ENDPOINT);
    return odps;
  }

  /**
   * Constructs an Odps client using an Aliyun Credential Provider.
   * Suitable for scenarios such as RAM role authorization on ECS instances.
   * ICredentialProvider is an AK-less authentication method provided by Alibaba Cloud (it actually provides a mechanism for generating and automatically rotating STS Tokens based on RamRole, and for ODPS, it still falls under STS Token-based authentication).
   * The aliyun-java-auth package provides many implementations of ICredentialProvider, such as DefaultCredentialProvider and RamRoleArnCredentialProvider; users can choose different implementations based on their needs.
   *
   * @param credentialProvider An instance of Aliyun Credential Provider.
   * @return An initialized Odps instance.
   */
  public static Odps buildWithCredentialProvider(ICredentialProvider credentialProvider) {
    Account account = new AklessAccount(credentialProvider);
    Odps odps = new Odps(account);
    odps.setEndpoint(SAMPLE_ENDPOINT);
    return odps;
  }

  /**
   * Constructs an Odps client using Dual-Signature Authentication.
   * Some applications require dual-signature authentication (which essentially uses one set of AKs for application identification and another for user identification).
   *
   * @param accessId     User's AccessId.
   * @param accessKey    User's AccessKey.
   * @param appAccessId  Application's AccessId.
   * @param appAccessKey Application's AccessKey.
   * @return An initialized Odps instance.
   */
  public static Odps buildWithDualSignature(String accessId, String accessKey, String appAccessId, String appAccessKey) {
    AppAccount appAccount = new AppAccount(new AliyunAccount(appAccessId, appAccessKey));
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account, appAccount);
    odps.setEndpoint(SAMPLE_ENDPOINT);
    return odps;
  }

  /**
   * Constructs an Odps client using Bearer Token.
   * Bearer Tokens are typically used for short-term access authorization.
   * How to generate Bearer Token: {@link com.aliyun.odps.security.SecurityManager#generateAuthorizationToken(String, String)}
   * Example: sm.generateAuthorizationToken(policy, "BEARER");
   *
   * @param bearerToken The generated Bearer Token string.
   * @return An initialized Odps instance.
   */
  public static Odps buildWithBearerToken(String bearerToken) {
    Account account = new BearerTokenAccount(bearerToken);
    Odps odps = new Odps(account);
    odps.setEndpoint(SAMPLE_ENDPOINT);
    return odps;
  }
}
