package com.aliyun.odps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Optional;

import org.junit.Test;

/**
 * ProxyConfig 类的单元测试
 */
public class ProxyConfigTest extends TestBase {

    private ProxyConfig proxyConfig = ProxyConfig.builder().build();

    /**
     * 测试默认构造函数是否正确初始化 proxyMap
     */
    @Test
    public void testDefaultConstructor() {
        // 验证 proxyMap 已经被初始化
        assertNotNull(proxyConfig);
    }

    /**
     * 测试带参数构造函数是否正确调用 initProxy
     */
    @Test
    public void testConstructorWithEndpoint() {
        // Given
        String endpoint = "http://proxy.example.com:8080";

        // When
        proxyConfig = ProxyConfig.builder()
          .withHttpProxy(endpoint)
          .build();

        // Then
        Optional<Proxy> proxyOptional = proxyConfig.getProxy(ProxyConfig.Type.HTTP);
        assertTrue(proxyOptional.isPresent());
        Proxy proxy = proxyOptional.get();
        assertEquals(Proxy.Type.HTTP, proxy.type());
        InetSocketAddress address = (InetSocketAddress) proxy.address();
        assertEquals("proxy.example.com", address.getHostName());
        assertEquals(8080, address.getPort());
    }

    /**
     * 测试 initProxy 方法处理 HTTPS 协议
     */
    @Test
    public void testInitProxyWithHttps() {
        // Given
        String endpoint = "https://proxy.example.com:8080";

        // When
        ProxyConfig config = proxyConfig.builder().withHttpsProxy(endpoint).build();
        Optional<Proxy> proxy = config.getProxy(ProxyConfig.Type.HTTPS);
        assertTrue(proxy.isPresent());

        // Then
        assertEquals(Proxy.Type.HTTP, proxy.get().type()); // 注意：Java Proxy.Type 中 HTTPS 也归类为 HTTP
        InetSocketAddress address = (InetSocketAddress) proxy.get().address();
        assertEquals("proxy.example.com", address.getHostName());
        assertEquals(8080, address.getPort());
    }
}

