package com.aliyun.odps.storage.internal.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.junit.Test;

import com.aliyun.odps.ProxyConfig;

public class OkHttpProxyHelperTest {

  @Test
  public void testSelectHttpProxy() throws URISyntaxException {
    ProxyConfig config = ProxyConfig.builder()
      .withHttpProxy("http://1.1.1.1:8080")
      .withHttpsProxy("http://2.2.2.2:8443")
      .build();

    ProxySelector selector = OkHttpProxyHelper.createProxySelector(config);

    List<Proxy> httpProxies = selector.select(new URI("http://odps.aliyun.com"));
    assertEquals(1, httpProxies.size());

    Proxy proxy = httpProxies.get(0);
    assertEquals(Proxy.Type.HTTP, proxy.type());

    // 使用更健壮的断言方式
    InetSocketAddress addr = (InetSocketAddress) proxy.address();
    assertEquals("1.1.1.1", addr.getHostString());
    assertEquals(8080, addr.getPort());
  }

  @Test
  public void testSelectHttpsProxy() throws URISyntaxException {
    ProxyConfig config = ProxyConfig.builder()
      .withHttpProxy("http://1.1.1.1:8080")
      .withHttpsProxy("http://2.2.2.2:8443")
      .build();

    ProxySelector selector = OkHttpProxyHelper.createProxySelector(config);

    List<Proxy> httpsProxies = selector.select(new URI("https://odps.aliyun.com"));
    assertEquals(1, httpsProxies.size());

    Proxy proxy = httpsProxies.get(0);
    assertEquals(Proxy.Type.HTTP, proxy.type());

    InetSocketAddress addr = (InetSocketAddress) proxy.address();
    assertEquals("2.2.2.2", addr.getHostString());
    assertEquals(8443, addr.getPort());
  }

  @Test
  public void testFallbackToSocks5() throws URISyntaxException {
    ProxyConfig config = ProxyConfig.builder()
      .withSocks5Proxy("http://3.3.3.3:1080")
      .build();

    ProxySelector selector = OkHttpProxyHelper.createProxySelector(config);

    List<Proxy> proxies = selector.select(new URI("http://odps.aliyun.com"));
    assertEquals(1, proxies.size());

    Proxy proxy = proxies.get(0);
    assertEquals(Proxy.Type.SOCKS, proxy.type());

    InetSocketAddress addr = (InetSocketAddress) proxy.address();
    assertEquals("3.3.3.3", addr.getHostString());
    assertEquals(1080, addr.getPort());
  }

  @Test
  public void testFallbackToSocks4() throws URISyntaxException {
    ProxyConfig config = ProxyConfig.builder()
      .withSocks4Proxy("http://4.4.4.4:1080")
      .build();

    ProxySelector selector = OkHttpProxyHelper.createProxySelector(config);

    List<Proxy> proxies = selector.select(new URI("https://odps.aliyun.com"));
    assertEquals(1, proxies.size());

    Proxy proxy = proxies.get(0);
    assertEquals(Proxy.Type.SOCKS, proxy.type());

    InetSocketAddress addr = (InetSocketAddress) proxy.address();
    assertEquals("4.4.4.4", addr.getHostString());
    assertEquals(1080, addr.getPort());
  }

  @Test
  public void testDefaultToDirect() throws URISyntaxException {
    ProxyConfig config = ProxyConfig.builder().build();
    ProxySelector selector = OkHttpProxyHelper.createProxySelector(config);

    List<Proxy> proxies = selector.select(new URI("http://odps.aliyun.com"));
    assertEquals(1, proxies.size());
    assertEquals(Proxy.NO_PROXY, proxies.get(0));
  }
}
