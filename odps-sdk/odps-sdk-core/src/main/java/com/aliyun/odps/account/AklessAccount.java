package com.aliyun.odps.account;

import com.aliyun.auth.credentials.ICredential;
import com.aliyun.auth.credentials.exception.CredentialException;
import com.aliyun.auth.credentials.provider.ICredentialProvider;
import com.aliyun.credentials.AlibabaCloudCredentials;
import com.aliyun.credentials.provider.AlibabaCloudCredentialsProvider;
import com.aliyun.odps.utils.StringUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class AklessAccount implements Account {

  enum ProviderType {
    /**
     * provided by aliyun-java-auth
     */
    ICredentialProvider,
    /**
     * provided by credentials-java
     */
    AlibabaCloudCredentialsProvider
  }

  private final ProviderType providerType;

  private ICredentialProvider credentialsProvider;
  private AlibabaCloudCredentialsProvider alibabaCloudCredentialsProvider;
  private String region;

  public AklessAccount(ICredentialProvider credentialsProvider) {
    this(credentialsProvider, null);
  }

  public AklessAccount(ICredentialProvider credentialsProvider, String region) {
    this.providerType = ProviderType.ICredentialProvider;
    this.credentialsProvider = credentialsProvider;
    this.region = region;
  }

  public AklessAccount(AlibabaCloudCredentialsProvider credentialsProvider) {
    this(credentialsProvider, null);
  }

  public AklessAccount(AlibabaCloudCredentialsProvider credentialsProvider, String region) {
    this.providerType = ProviderType.AlibabaCloudCredentialsProvider;
    this.alibabaCloudCredentialsProvider = credentialsProvider;
    this.region = region;
  }

  @Override
  public AccountProvider getType() {
    // not exactly, when use AlibabaCloudCredentialsProvider, it may bearer_token
    return AccountProvider.STS;
  }

  /**
   * Set regionId
   * Can be used to upgrade the signature verification method to v4 signature
   * @param region
   */
  public void setRegion(String region) {
    this.region = region;
  }

  @Override
  public RequestSigner getRequestSigner() {
    try {
      switch (providerType) {
        case ICredentialProvider:
          ICredential credentials = credentialsProvider.getCredentials();
          return new StsRequestSigner(credentials.accessKeyId(),
                                      credentials.accessKeySecret(),
                                      credentials.securityToken(),
                                      region);
        case AlibabaCloudCredentialsProvider:
          AlibabaCloudCredentials
              alibabaCloudCredentials =
              alibabaCloudCredentialsProvider.getCredentials();
          if (StringUtils.isNotBlank(alibabaCloudCredentials.getBearerToken())) {
            return new BearerTokenRequestSigner(alibabaCloudCredentials.getBearerToken());
          } else {
            return new StsRequestSigner(alibabaCloudCredentials.getAccessKeyId(),
                                        alibabaCloudCredentials.getAccessKeySecret(),
                                        alibabaCloudCredentials.getSecurityToken(),
                                        region);
          }
        default:
          throw new RuntimeException("Unsupported provider type: " + providerType);
      }
    } catch (CredentialException e) {
      throw new RuntimeException(e);
    }
  }
}
