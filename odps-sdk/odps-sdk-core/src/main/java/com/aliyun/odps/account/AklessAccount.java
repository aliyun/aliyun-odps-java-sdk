package com.aliyun.odps.account;

import com.aliyun.auth.credentials.ICredential;
import com.aliyun.auth.credentials.exception.CredentialException;
import com.aliyun.auth.credentials.provider.ICredentialProvider;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class AklessAccount implements Account {

  private final ICredentialProvider credentialsProvider;

  public AklessAccount(ICredentialProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  public AccountProvider getType() {
    return AccountProvider.STS;
  }

  @Override
  public RequestSigner getRequestSigner() {
    try {
      ICredential credentials = credentialsProvider.getCredentials();
      return new StsRequestSigner(credentials.accessKeyId(), credentials.accessKeySecret(),
                                  credentials.securityToken());
    } catch (CredentialException e) {
      throw new RuntimeException(e);
    }
  }
}
