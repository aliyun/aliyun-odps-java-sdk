/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.table.transport;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.transport.Transport;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.RestWriteTimeoutException;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TableTransportTest {

    @Test
    public void testDefaultConnectionUsesJdk() throws Exception {
        RecordingTransport transport = new RecordingTransport();
        TableTransport tableTransport = new TableTransport(transport, null);
        Request request = request(
                new RestClient(tableTransport), URI.create("http://localhost/upload"));

        Connection connection = tableTransport.connect(request);

        Assert.assertTrue(connection instanceof JdkConnection);
        connection.getOutputStream().write(1);
        Assert.assertEquals(1, transport.connection.body.size());
    }

    @Test
    public void testConfiguredWriteTimeoutUsesOkHttpAndStreamsResponse() throws Exception {
        MockWebServer server = new MockWebServer();
        server.enqueue(new MockResponse()
                .setResponseCode(200)
                .addHeader(Headers.ODPS_REQUEST_ID, "request-id")
                .setBody("commit-result"));
        server.start();
        Connection connection = null;
        try {
            RestClient restClient = configuredClient(server.url("/").toString(), 2);
            restClient.setConnectTimeout(2);
            restClient.setReadTimeout(2);
            Request request = request(restClient, server.url("/upload").uri());
            request.setHeader("x-odps-signed-test", "signed-value");
            connection = restClient.getTransport().connect(request);

            Assert.assertTrue(connection instanceof OkHttpConnection);
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write("arrow-payload".getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
            outputStream.close();

            Response response = connection.getResponse();
            Assert.assertTrue(response.isOK());
            Assert.assertEquals("request-id", response.getHeader(Headers.ODPS_REQUEST_ID));
            Assert.assertEquals(
                    "commit-result",
                    readFully(connection.getInputStream()));

            RecordedRequest recordedRequest = server.takeRequest(2, TimeUnit.SECONDS);
            Assert.assertNotNull(recordedRequest);
            Assert.assertEquals("arrow-payload", recordedRequest.getBody().readUtf8());
            Assert.assertEquals("chunked", recordedRequest.getHeader(Headers.TRANSFER_ENCODING));
            Assert.assertEquals(
                    "signed-value", recordedRequest.getHeader("x-odps-signed-test"));
            Assert.assertEquals(1, server.getRequestCount());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            server.shutdown();
        }
    }

    @Test
    public void testStalledSocketTimesOutWithoutConnectionRetry() throws Exception {
        ServerSocket serverSocket = new ServerSocket();
        serverSocket.setReceiveBufferSize(1024);
        serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
        AtomicInteger acceptedConnections = new AtomicInteger();
        AtomicReference<Socket> acceptedSocket = new AtomicReference<>();
        CountDownLatch accepted = new CountDownLatch(1);
        CountDownLatch releaseServer = new CountDownLatch(1);
        Thread serverThread = startStalledServer(
                serverSocket, acceptedConnections, acceptedSocket, accepted, releaseServer);

        Connection connection = null;
        try {
            RestClient restClient = configuredClient(
                    "http://127.0.0.1:" + serverSocket.getLocalPort(), 1);
            restClient.setConnectTimeout(2);
            restClient.setReadTimeout(2);
            URI uri = URI.create("http://127.0.0.1:"
                    + serverSocket.getLocalPort() + "/upload");
            connection = restClient.getTransport().connect(request(restClient, uri));
            OutputStream outputStream = connection.getOutputStream();
            Assert.assertTrue(accepted.await(2, TimeUnit.SECONDS));

            long startNanos = System.nanoTime();
            RestWriteTimeoutException timeout = assertWriteTimeout(outputStream);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startNanos);

            Assert.assertEquals(1, timeout.getTimeoutSeconds());
            Assert.assertTrue(timeout.getBytesProduced() > 0);
            Assert.assertTrue("write timeout took " + elapsedMillis + "ms", elapsedMillis < 8000);
            Assert.assertEquals(1, acceptedConnections.get());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            releaseServer.countDown();
            Socket socket = acceptedSocket.get();
            if (socket != null) {
                socket.close();
            }
            serverSocket.close();
            serverThread.join(2000);
        }
    }

    @Test
    public void testConfiguredTransportSendsControlPlaneRequestBody() throws Exception {
        MockWebServer server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(200).setBody("session-result"));
        server.start();
        try {
            RestClient restClient = configuredClient(server.url("/").toString(), 2);
            byte[] requestBody = "{\"SessionType\":\"batch_read\"}"
                    .getBytes(StandardCharsets.UTF_8);
            Request request = new Request(restClient);
            request.setURI(server.url("/sessions").uri());
            request.setMethod(Request.Method.POST);
            request.setHeader(Headers.CONTENT_TYPE, "application/json");
            request.setBody(new ByteArrayInputStream(requestBody));
            request.setBodyLength(requestBody.length);

            Response response = restClient.getTransport().request(request);

            Assert.assertTrue(response.isOK());
            Assert.assertEquals(
                    "session-result",
                    new String(response.getBody(), StandardCharsets.UTF_8));
            RecordedRequest recordedRequest = server.takeRequest(2, TimeUnit.SECONDS);
            Assert.assertNotNull(recordedRequest);
            Assert.assertEquals("POST", recordedRequest.getMethod());
            Assert.assertEquals(requestBody.length, recordedRequest.getBodySize());
            Assert.assertEquals(
                    String.valueOf(requestBody.length),
                    recordedRequest.getHeader(Headers.CONTENT_LENGTH));
        } finally {
            server.shutdown();
        }
    }

    @Test
    public void testConfiguredTransportOpensReaderGetWithOkHttp() throws Exception {
        MockWebServer server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(200).setBody("arrow-batches"));
        server.start();
        Connection connection = null;
        try {
            RestClient restClient = configuredClient(server.url("/").toString(), 2);
            Request request = new Request(restClient);
            request.setURI(server.url("/data").uri());
            request.setMethod(Request.Method.GET);
            connection = restClient.getTransport().connect(request);

            Assert.assertTrue(connection instanceof OkHttpConnection);
            Assert.assertTrue(connection.getResponse().isOK());
            Assert.assertEquals("arrow-batches", readFully(connection.getInputStream()));
            RecordedRequest recordedRequest = server.takeRequest(2, TimeUnit.SECONDS);
            Assert.assertNotNull(recordedRequest);
            Assert.assertEquals("GET", recordedRequest.getMethod());
            Assert.assertEquals(0L, recordedRequest.getBodySize());
            Assert.assertNull(recordedRequest.getHeader(Headers.ACCEPT_ENCODING));
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            server.shutdown();
        }
    }

    @Test
    public void testConfiguredTransportPreservesExplicitReaderCompression() throws Exception {
        MockWebServer server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(200).setBody("compressed-batches"));
        server.start();
        Connection connection = null;
        try {
            RestClient restClient = configuredClient(server.url("/").toString(), 2);
            Request request = new Request(restClient);
            request.setURI(server.url("/data").uri());
            request.setMethod(Request.Method.GET);
            request.setHeader(Headers.ACCEPT_ENCODING, "ZSTD");
            connection = restClient.getTransport().connect(request);

            Assert.assertTrue(connection.getResponse().isOK());
            Assert.assertEquals("compressed-batches", readFully(connection.getInputStream()));
            RecordedRequest recordedRequest = server.takeRequest(2, TimeUnit.SECONDS);
            Assert.assertNotNull(recordedRequest);
            Assert.assertEquals("ZSTD", recordedRequest.getHeader(Headers.ACCEPT_ENCODING));
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            server.shutdown();
        }
    }

    @Test
    public void testConfiguredTransportDoesNotRemoveExplicitGzip() throws Exception {
        MockWebServer server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(200).setBody("gzip-batches"));
        server.start();
        Connection connection = null;
        try {
            RestClient restClient = configuredClient(server.url("/").toString(), 2);
            Request request = new Request(restClient);
            request.setURI(server.url("/data").uri());
            request.setMethod(Request.Method.GET);
            request.setHeader(Headers.ACCEPT_ENCODING, "gzip");
            connection = restClient.getTransport().connect(request);

            Assert.assertTrue(connection.getResponse().isOK());
            Assert.assertEquals("gzip-batches", readFully(connection.getInputStream()));
            RecordedRequest recordedRequest = server.takeRequest(2, TimeUnit.SECONDS);
            Assert.assertNotNull(recordedRequest);
            Assert.assertEquals("gzip", recordedRequest.getHeader(Headers.ACCEPT_ENCODING));
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            server.shutdown();
        }
    }

    @Test
    public void testWriteTimeoutMustBePositive() {
        Assert.assertEquals(
                Integer.valueOf(3),
                RestOptions.newBuilder().withWriteTimeout(3).build()
                        .getWriteTimeout().get());
        try {
            RestOptions.newBuilder().withWriteTimeout(0);
            Assert.fail("Expected invalid write timeout");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("greater than 0"));
        }
    }

    private static RestWriteTimeoutException assertWriteTimeout(OutputStream stream)
            throws IOException {
        byte[] data = new byte[256 * 1024];
        try {
            for (int index = 0; index < 1024; index++) {
                stream.write(data);
            }
            Assert.fail("Expected write timeout");
            return null;
        } catch (RestWriteTimeoutException expected) {
            return expected;
        }
    }

    private static Thread startStalledServer(ServerSocket serverSocket,
                                             AtomicInteger acceptedConnections,
                                             AtomicReference<Socket> acceptedSocket,
                                             CountDownLatch accepted,
                                             CountDownLatch releaseServer) {
        Thread thread = new Thread(() -> {
            try {
                Socket socket = serverSocket.accept();
                socket.setReceiveBufferSize(1024);
                acceptedSocket.set(socket);
                acceptedConnections.incrementAndGet();
                accepted.countDown();
                releaseServer.await(15, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                accepted.countDown();
            }
        }, "table-api-stalled-server");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static Request request(RestClient restClient, URI uri) {
        Request request = new Request(restClient);
        request.setURI(uri);
        request.setMethod(Request.Method.POST);
        request.setHeader(Headers.CONTENT_TYPE, "application/octet-stream");
        request.setHeader(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
        return request;
    }

    private static RestClient configuredClient(String endpoint, int writeTimeoutSeconds) {
        EnvironmentSettings settings = EnvironmentSettings.newBuilder()
                .inRemoteMode()
                .withCredentials(Credentials.newBuilder()
                        .withAccount(new AliyunAccount("access-id", "access-key"))
                        .build())
                .withServiceEndpoint(endpoint)
                .withTunnelEndpoint(endpoint)
                .withRestOptions(RestOptions.newBuilder()
                        .withWriteTimeout(writeTimeoutSeconds)
                        .build())
                .build();
        return ExecutionEnvironment.create(settings).createHttpClient("project");
    }

    private static String readFully(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) >= 0) {
            outputStream.write(buffer, 0, length);
        }
        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    }

    private static final class RecordingTransport implements Transport {

        private final RecordingConnection connection = new RecordingConnection();

        @Override
        public Response request(Request request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection connect(Request request) {
            return connection;
        }

        @Override
        public void setProxy(java.net.Proxy proxy) {
            // No-op for the default-selection unit test.
        }
    }

    private static final class RecordingConnection implements Connection {

        private final ByteArrayOutputStream body = new ByteArrayOutputStream();

        @Override
        public void connect(Request request) {
            // The recording transport returns an already-connected test connection.
        }

        @Override
        public OutputStream getOutputStream() {
            return body;
        }

        @Override
        public Response getResponse() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public void disconnect() {
            // No-op for the default-selection unit test.
        }
    }
}
