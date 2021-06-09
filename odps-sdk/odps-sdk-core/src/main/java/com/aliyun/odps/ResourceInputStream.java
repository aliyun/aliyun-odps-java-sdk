package com.aliyun.odps;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.RestClient;


/**
 * A wrapper class. All the method calls are passed directly to the class member 'inputStream'.
 * This class ensures the {@link Connection} will be closed.
 */
public class ResourceInputStream extends InputStream {

  private Connection conn;
  private InputStream inputStream;

  ResourceInputStream(
      RestClient client,
      String projectName,
      String resourceName) throws OdpsException {

    String resource = String.format("/projects/%s/resources/%s", projectName, resourceName);
    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/octet-stream");

    try {
      conn = client.connect(resource, "GET", null, headers);
      Response resp = conn.getResponse();
      inputStream = conn.getInputStream();
      if (!resp.isOK()) {
        String message = new String(IOUtils.readFully(inputStream));
        conn.disconnect();
        throw new OdpsException(message);
      }
    } catch (IOException e) {
      if (conn != null) {
        try {
          conn.disconnect();
        } catch (IOException ignore) {
        }
      }
      throw new OdpsException(e);
    }
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  @Override
  public synchronized void mark(int readlimit) {
    inputStream.mark(readlimit);
  }

  @Override
  public boolean markSupported() {
    return inputStream.markSupported();
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return inputStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return inputStream.read(b, off, len);
  }

  @Override
  public synchronized void reset() throws IOException {
    inputStream.reset();
  }

  @Override
  public long skip(long n) throws IOException {
    return inputStream.skip(n);
  }

  @Override
  public void close() throws IOException {
    super.close();
    inputStream.close();
    conn.disconnect();
  }
}