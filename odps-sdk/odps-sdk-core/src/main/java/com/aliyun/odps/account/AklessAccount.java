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

  public AklessAccount(ICredentialProvider credentialsProvider) {
    this.providerType = ProviderType.ICredentialProvider;
    this.credentialsProvider = credentialsProvider;
  }

  public AklessAccount(AlibabaCloudCredentialsProvider credentialsProvider) {
    this.providerType = ProviderType.AlibabaCloudCredentialsProvider;
    this.alibabaCloudCredentialsProvider = credentialsProvider;
  }

  @Override
  public AccountProvider getType() {
    // not exactly, when use AlibabaCloudCredentialsProvider, it may bearer_token
    return AccountProvider.STS;
  }

  @Override
  public RequestSigner getRequestSigner() {
    try {
      switch (providerType) {
        case ICredentialProvider:
          ICredential credentials = credentialsProvider.getCredentials();
          return new StsRequestSigner(credentials.accessKeyId(),
                                      credentials.accessKeySecret(),
                                      credentials.securityToken());
        case AlibabaCloudCredentialsProvider:
          AlibabaCloudCredentials
              alibabaCloudCredentials =
              alibabaCloudCredentialsProvider.getCredentials();
          if (StringUtils.isNotBlank(alibabaCloudCredentials.getBearerToken())) {
            return new BearerTokenRequestSigner(alibabaCloudCredentials.getBearerToken());
          } else {
            return new StsRequestSigner(alibabaCloudCredentials.getAccessKeyId(),
                                        alibabaCloudCredentials.getAccessKeySecret(),
                                        alibabaCloudCredentials.getSecurityToken());
          }
        default:
          throw new RuntimeException("Unsupported provider type: " + providerType);
      }
    } catch (CredentialException e) {
      throw new RuntimeException(e);
    }
  }
}
