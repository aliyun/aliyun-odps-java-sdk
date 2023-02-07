package com.aliyun.odps;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Params;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.utils.StringUtils;


/**
 * A wrapper class. All the method calls are passed directly to the class member 'inputStream'. This
 * class ensures the {@link Connection} will be closed.
 */
public class ResourceInputStream extends InputStream {

  private Connection conn;
  private InputStream inputStream;
  private long offset = 0;
  private final String resource;
  private final String schemaName;
  private final RestClient client;
  private boolean hasRemainingContentToFetch;

  private long chunkSize;
  private static final long MAX_SKIP_BUFFER_SIZE = 2048;

  ResourceInputStream(
      RestClient client,
      String projectName,
      String schemaName,
      String resourceName) throws OdpsException {

    this.client = client;
    this.hasRemainingContentToFetch = false;
    this.resource = String.format("/projects/%s/resources/%s", projectName, resourceName);
    this.schemaName = schemaName;
    this.chunkSize = 64L << 20;  // default 64M

    try {
      resetInputStream(true);
    } catch (Exception e) {
      throw new OdpsException(e);
    }
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public int read() throws IOException {
    byte[] buff = new byte[1];
    int res = read(buff, 0, 1);
    return res == -1 ? -1 : buff[0];
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int readSize = inputStream.read(b, off, len);
    if (readSize == -1 && hasRemainingContentToFetch) {
      resetInputStream(false);
      readSize = inputStream.read(b, off, len);
    }
    if (readSize > 0) {
      offset += readSize;
    }
    return readSize;
  }

  @Override
  public long skip(long n) throws IOException {

    long remaining = n;
    int nr;

    if (n <= 0) {
      return 0;
    }

    int size = (int) Math.min(MAX_SKIP_BUFFER_SIZE, remaining);
    byte[] skipBuffer = new byte[size];
    while (remaining > 0) {
      nr = read(skipBuffer, 0, (int) Math.min(size, remaining));
      if (nr < 0) {
        break;
      }
      remaining -= nr;
    }

    return n - remaining;
  }

  @Override
  public void close() throws IOException {
    super.close();
    inputStream.close();
    conn.disconnect();
  }

  private void resetInputStream(boolean init) throws IOException {
    if (!init) {
      close();
    }

    Map<String, String> headers = new HashMap<>();
    headers.put(Headers.CONTENT_TYPE, "application/octet-stream");

    Map<String, String> params = new HashMap<>();
    params.put(Params.ODPS_RESOURCE_FETCH_OFFSET, String.valueOf(offset));
    params.put(Params.ODPS_RESOURCE_FETCH_READ_SIZE, String.valueOf(chunkSize));
    if (!StringUtils.isNullOrEmpty(schemaName)) {
      params.put(Params.ODPS_SCHEMA_NAME, schemaName);
    }

    try {
      conn = client.connect(resource, "GET", params, headers);
      Response resp = conn.getResponse();
      inputStream = conn.getInputStream();
      if (!resp.isOK()) {
        String message = new String(IOUtils.readFully(inputStream));
        conn.disconnect();
        throw new OdpsException(message);
      }
      String hasRemaining = resp.getHeader(Headers.ODPS_RESOURCE_HAS_REMAINING_CONTENT);
      this.hasRemainingContentToFetch =
          hasRemaining != null && hasRemaining.equalsIgnoreCase("true");
    } catch (IOException | OdpsException e) {
      if (conn != null) {
        try {
          conn.disconnect();
        } catch (IOException ignore) {
        }
      }
      throw new IOException(e);
    }
  }

  protected void setChunkSize(long chunkSize) {
    this.chunkSize = chunkSize;
  }
}