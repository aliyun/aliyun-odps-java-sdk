package com.aliyun.odps.account;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.commons.transport.Request;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class AliyunV4AuthTest {

  static Properties props;
  static String accessId;
  static String accessKey;
  static String endpoint;
  static String project;

  @BeforeClass
  public static void setup() throws IOException {
    props = OdpsTestUtils.loadConfig();
    accessId = props.getProperty("default.access.id");
    accessKey = props.getProperty("default.access.key");
    endpoint = props.getProperty("default.endpoint");
    project = props.getProperty("default.project");
  }

  @Test
  public void testV4Sign() throws URISyntaxException, OdpsException {
    AliyunAccount aliyunAccount = new AliyunAccount(accessId, accessKey, "cn-hangzhou");
    Request request = new Request();
    request.setURI(new URI("http://www.aliyun.com"));
    aliyunAccount.getRequestSigner().sign("resource", request);
    String authorization = request.getHeaders().get("Authorization");
    Assert.assertTrue(authorization.contains("aliyun_v4_request"));

    Odps odps = new Odps(aliyunAccount);
    odps.setEndpoint(endpoint);
    odps.projects().get(project).reload();
  }

  @Test
  public void testV2Sign() throws URISyntaxException, OdpsException {
    AliyunAccount aliyunAccount = new AliyunAccount(accessId, accessKey);
    Request request = new Request();
    request.setURI(new URI("http://www.aliyun.com"));
    aliyunAccount.getRequestSigner().sign("resource", request);
    String authorization = request.getHeaders().get("Authorization");
    Assert.assertFalse(authorization.contains("aliyun_v4_request"));

    Odps odps = new Odps(aliyunAccount);
    odps.setEndpoint(endpoint);
    odps.projects().get(project).reload();
  }

  @Test
  public void testStsAccount() throws URISyntaxException, OdpsException {
    StsAccount withRegionAccount = new StsAccount(accessId, accessKey, "stsToken", "cn-hangzhou");
    Request request = new Request();
    request.setURI(new URI("http://www.aliyun.com"));
    withRegionAccount.getRequestSigner().sign("resource", request);
    String authorizationV4 = request.getHeaders().get("Authorization");
    Assert.assertTrue(authorizationV4.contains("aliyun_v4_request"));

    StsAccount noRegionAccount = new StsAccount(accessId, accessKey, "stsToken");
    request = new Request();
    request.setURI(new URI("http://www.aliyun.com"));
    noRegionAccount.getRequestSigner().sign("resource", request);
    String authorizationV2 = request.getHeaders().get("Authorization");
    Assert.assertFalse(authorizationV2.contains("aliyun_v4_request"));
  }
}
