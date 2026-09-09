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

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.RestWriteTimeoutException;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Dispatcher;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okio.BufferedSink;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** OkHttp connection used by Table API when a write timeout is explicitly configured. */
final class OkHttpConnection implements Connection {

    private static final int OUTPUT_BUFFER_SIZE = 64 * 1024;
    private static final int QUEUE_CAPACITY = 8;
    private static final AutomaticAcceptEncoding AUTOMATIC_ACCEPT_ENCODING =
            new AutomaticAcceptEncoding();
    private static final OkHttpClient BASE_CLIENT = new OkHttpClient.Builder()
            .dispatcher(createDispatcher())
            .addInterceptor(chain -> {
                okhttp3.Request request = chain.request();
                if (request.header(Headers.ACCEPT_ENCODING) == null) {
                    request = request.newBuilder()
                            .tag(AutomaticAcceptEncoding.class, AUTOMATIC_ACCEPT_ENCODING)
                            .build();
                }
                return chain.proceed(request);
            })
            .addNetworkInterceptor(chain -> {
                okhttp3.Request request = chain.request();
                if (request.tag(AutomaticAcceptEncoding.class) != null) {
                    request = request.newBuilder()
                            .removeHeader(Headers.ACCEPT_ENCODING)
                            .build();
                }
                return chain.proceed(request);
            })
            .followRedirects(false)
            .followSslRedirects(false)
            .retryOnConnectionFailure(false)
            .protocols(Collections.singletonList(Protocol.HTTP_1_1))
            .build();

    private static final class AutomaticAcceptEncoding {
    }

    private final int writeTimeoutSeconds;
    private final BlockingQueue<WriteFrame> frames =
            new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final CompletableFuture<okhttp3.Response> responseFuture =
            new CompletableFuture<>();
    private final AtomicReference<IOException> failure = new AtomicReference<>();
    private final AtomicLong bytesProduced = new AtomicLong();

    private volatile Call call;
    private volatile okhttp3.Response response;
    private volatile boolean requestBodyAllowed;
    private volatile boolean requestBodyComplete;
    private OutputStream outputStream;

    OkHttpConnection(int writeTimeoutSeconds) {
        this.writeTimeoutSeconds = writeTimeoutSeconds;
    }

    @Override
    public void connect(Request request) throws IOException {
        if (call != null) {
            throw new IOException("OKHTTP connection is already connected");
        }

        RestClient restClient = request.getRestClient();
        OkHttpClient.Builder clientBuilder = BASE_CLIENT.newBuilder()
                .connectTimeout(restClient.getConnectTimeout(), TimeUnit.SECONDS)
                .readTimeout(restClient.getReadTimeout(), TimeUnit.SECONDS)
                .writeTimeout(writeTimeoutSeconds, TimeUnit.SECONDS);
        if (restClient.getProxy() != null) {
            clientBuilder.proxy(restClient.getProxy());
        }
        if (restClient.isIgnoreCerts()) {
            configureInsecureTls(clientBuilder);
        }

        okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
                .url(request.getURI().toString());
        for (Map.Entry<String, String> header : request.getHeaders().entrySet()) {
            requestBuilder.header(header.getKey(), header.getValue());
        }

        String contentType = request.getHeaders().get(Headers.CONTENT_TYPE);
        requestBodyAllowed = allowsRequestBody(request);
        RequestBody requestBody = null;
        if (requestBodyAllowed) {
            requestBody = new StreamingRequestBody(
                    contentType == null ? null : MediaType.parse(contentType),
                    contentLength(request));
        } else {
            requestBodyComplete = true;
        }
        requestBuilder.method(request.getMethod().name(), requestBody);

        call = clientBuilder.build().newCall(requestBuilder.build());
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call failedCall, IOException exception) {
                IOException existingFailure = failure.get();
                fail(existingFailure == null ? exception : existingFailure);
            }

            @Override
            public void onResponse(Call completedCall, okhttp3.Response completedResponse) {
                if (!responseFuture.complete(completedResponse)) {
                    completedResponse.close();
                }
            }
        });
    }

    @Override
    public synchronized OutputStream getOutputStream() throws IOException {
        ensureConnected();
        if (!requestBodyAllowed) {
            throw new IOException("HTTP " + requestMethod() + " does not support a request body");
        }
        checkFailure();
        if (outputStream == null) {
            outputStream = new BufferedOutputStream(
                    new QueueOutputStream(), OUTPUT_BUFFER_SIZE);
        }
        return outputStream;
    }

    @Override
    public Response getResponse() throws IOException {
        ensureConnected();
        if (!requestBodyComplete) {
            getOutputStream().close();
        }
        try {
            response = responseFuture.get();
            return new OkHttpResponse(response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for REST response", e);
        } catch (ExecutionException e) {
            throw asIOException(e.getCause());
        }
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (response == null) {
            getResponse();
        }
        ResponseBody body = response.body();
        return body == null ? new ByteArrayInputStream(new byte[0]) : body.byteStream();
    }

    @Override
    public void disconnect() {
        Call currentCall = call;
        if (currentCall != null && !responseFuture.isDone()) {
            fail(new IOException("OKHTTP connection disconnected"));
        } else if (currentCall != null) {
            currentCall.cancel();
        }
        okhttp3.Response currentResponse = response;
        if (currentResponse != null) {
            currentResponse.close();
        }
    }

    private void ensureConnected() throws IOException {
        if (call == null) {
            throw new IOException("OKHTTP connection has not been connected");
        }
    }

    private String requestMethod() {
        return call.request().method();
    }

    private static boolean allowsRequestBody(Request request) {
        switch (request.getMethod()) {
            case POST:
            case PUT:
                return true;
            case DELETE:
                return request.getBody() != null || request.getBodyLength() > 0;
            default:
                return false;
        }
    }

    private static long contentLength(Request request) {
        if (request.getBody() != null) {
            return request.getBodyLength();
        }
        String transferEncoding = request.getHeaders().get(Headers.TRANSFER_ENCODING);
        if (Headers.CHUNKED.equalsIgnoreCase(transferEncoding)) {
            return -1L;
        }
        String contentLength = request.getHeaders().get(Headers.CONTENT_LENGTH);
        if (contentLength == null) {
            return -1L;
        }
        try {
            return Long.parseLong(contentLength);
        } catch (NumberFormatException ignored) {
            return -1L;
        }
    }

    private void checkFailure() throws IOException {
        IOException exception = failure.get();
        if (exception != null) {
            throw exception;
        }
    }

    private void offer(WriteFrame frame) throws IOException {
        checkFailure();
        try {
            frames.put(frame);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while streaming REST request", e);
        }
        checkFailure();
    }

    private void waitFor(WriteFrame frame) throws IOException {
        try {
            frame.acknowledged.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while flushing REST request", e);
        } catch (ExecutionException e) {
            throw asIOException(e.getCause());
        }
    }

    private RestWriteTimeoutException writeTimeout(Throwable cause) {
        return new RestWriteTimeoutException(
                writeTimeoutSeconds,
                bytesProduced.get(),
                cause);
    }

    private IOException mapWriteFailure(IOException exception) {
        if (exception instanceof RestWriteTimeoutException) {
            return exception;
        }
        if (hasSocketTimeoutCause(exception)) {
            return writeTimeout(exception);
        }
        return exception;
    }

    private static boolean hasSocketTimeoutCause(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SocketTimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void fail(IOException exception) {
        if (!failure.compareAndSet(null, exception)) {
            return;
        }
        List<WriteFrame> pendingFrames = new ArrayList<>();
        frames.drainTo(pendingFrames);
        for (WriteFrame frame : pendingFrames) {
            frame.acknowledged.completeExceptionally(exception);
        }
        frames.offer(WriteFrame.cancel(exception));
        responseFuture.completeExceptionally(exception);
        Call currentCall = call;
        if (currentCall != null) {
            currentCall.cancel();
        }
    }

    private static IOException asIOException(Throwable throwable) {
        if (throwable instanceof IOException) {
            return (IOException) throwable;
        }
        return new IOException(throwable == null ? null : throwable.getMessage(), throwable);
    }

    private static void configureInsecureTls(OkHttpClient.Builder builder) throws IOException {
        try {
            X509TrustManager trustManager = new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    // Trust all certificates only when RestOptions.ignoreCerts is enabled.
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    // Trust all certificates only when RestOptions.ignoreCerts is enabled.
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            };
            TrustManager[] trustManagers = new TrustManager[]{trustManager};
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagers, new SecureRandom());
            SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            builder.sslSocketFactory(socketFactory, trustManager);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, javax.net.ssl.SSLSession session) {
                    return true;
                }
            });
        } catch (GeneralSecurityException e) {
            throw new IOException("Unable to configure ignored HTTPS certificates", e);
        }
    }

    private static Dispatcher createDispatcher() {
        Dispatcher dispatcher = new Dispatcher();
        // Callers control Table API concurrency. Do not add OkHttp's per-host queue in front of
        // active requests, because time spent waiting in that queue is not a socket write timeout.
        dispatcher.setMaxRequests(Integer.MAX_VALUE);
        dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
        return dispatcher;
    }

    private final class StreamingRequestBody extends RequestBody {

        private final MediaType contentType;
        private final long contentLength;

        private StreamingRequestBody(MediaType contentType, long contentLength) {
            this.contentType = contentType;
            this.contentLength = contentLength;
        }

        @Override
        public MediaType contentType() {
            return contentType;
        }

        @Override
        public long contentLength() {
            return contentLength;
        }

        @Override
        public boolean isOneShot() {
            return true;
        }

        @Override
        public void writeTo(BufferedSink sink) throws IOException {
            WriteFrame currentFrame = null;
            try {
                while (true) {
                    currentFrame = frames.take();
                    if (currentFrame.type == FrameType.DATA) {
                        sink.write(currentFrame.data);
                    } else if (currentFrame.type == FrameType.FLUSH) {
                        sink.flush();
                        currentFrame.acknowledged.complete(null);
                    } else if (currentFrame.type == FrameType.END) {
                        sink.flush();
                        requestBodyComplete = true;
                        currentFrame.acknowledged.complete(null);
                        return;
                    } else {
                        throw currentFrame.failure;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                IOException exception = new IOException(
                        "Interrupted while sending REST request", e);
                fail(exception);
                throw exception;
            } catch (IOException e) {
                IOException exception = mapWriteFailure(e);
                if (currentFrame != null) {
                    currentFrame.acknowledged.completeExceptionally(exception);
                }
                fail(exception);
                throw exception;
            }
        }
    }

    private final class QueueOutputStream extends OutputStream {

        private boolean closed;

        @Override
        public void write(int value) throws IOException {
            write(new byte[]{(byte) value}, 0, 1);
        }

        @Override
        public void write(byte[] data, int offset, int length) throws IOException {
            if (closed) {
                throw new IOException("OKHTTP output stream is closed");
            }
            int currentOffset = offset;
            int remaining = length;
            while (remaining > 0) {
                int frameLength = Math.min(remaining, OUTPUT_BUFFER_SIZE);
                byte[] copy = Arrays.copyOfRange(
                        data, currentOffset, currentOffset + frameLength);
                offer(WriteFrame.data(copy));
                bytesProduced.addAndGet(frameLength);
                currentOffset += frameLength;
                remaining -= frameLength;
            }
        }

        @Override
        public void flush() throws IOException {
            if (closed) {
                return;
            }
            WriteFrame frame = WriteFrame.flush();
            offer(frame);
            waitFor(frame);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            flush();
            WriteFrame frame = WriteFrame.end();
            offer(frame);
            waitFor(frame);
            closed = true;
        }
    }

    private enum FrameType {
        DATA,
        FLUSH,
        END,
        CANCEL
    }

    private static final class WriteFrame {

        private final FrameType type;
        private final byte[] data;
        private final IOException failure;
        private final CompletableFuture<Void> acknowledged = new CompletableFuture<>();

        private WriteFrame(FrameType type, byte[] data, IOException failure) {
            this.type = type;
            this.data = data;
            this.failure = failure;
        }

        private static WriteFrame data(byte[] data) {
            return new WriteFrame(FrameType.DATA, data, null);
        }

        private static WriteFrame flush() {
            return new WriteFrame(FrameType.FLUSH, null, null);
        }

        private static WriteFrame end() {
            return new WriteFrame(FrameType.END, null, null);
        }

        private static WriteFrame cancel(IOException failure) {
            return new WriteFrame(FrameType.CANCEL, null, failure);
        }
    }

    private static final class OkHttpResponse extends Response {

        private OkHttpResponse(okhttp3.Response response) {
            this.status = response.code();
            this.message = response.message();
            for (String name : response.headers().names()) {
                List<String> values = response.headers(name);
                this.headers.put(Headers.toCaseSensitiveHeaderName(name), join(values));
            }
        }

        private static String join(List<String> values) {
            StringBuilder result = new StringBuilder();
            for (String value : values) {
                if (result.length() > 0) {
                    result.append(',');
                }
                result.append(value);
            }
            return result.toString();
        }
    }
}
