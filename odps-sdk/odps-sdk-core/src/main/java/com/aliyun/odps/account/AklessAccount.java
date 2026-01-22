package com.aliyun.odps.account;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class AklessAccount implements Account {

  private ICredentialsProvider credentialsProvider;
  private String region;

  public AklessAccount(ICredentialsProvider credentialsProvider) {
    this(credentialsProvider, null);
  }

  public AklessAccount(ICredentialsProvider credentialsProvider, String region) {
    this.credentialsProvider = credentialsProvider;
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
    ICredentials credentials = credentialsProvider.getCredentials();
    return new StsRequestSigner(credentials.getAccessKeyId(),
                                credentials.getAccessKeySecret(),
                                credentials.getSecurityToken(),
                                region);
  }

  @Override
  public ICredentials getCredentials() {
    return credentialsProvider.getCredentials();
  }

  @Override
  public String getRegionId() {
    return region;
  }

  public ICredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }
}
