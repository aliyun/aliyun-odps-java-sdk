package com.aliyun.odps.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.rest.RestClient;

/**
 * Created by yinyue on 15-6-8.
 */
@Deprecated
public class StreamUploadClient {

  private String projectName;
  private String tableName;
  private TunnelTableSchema schema;

  private Configuration conf;
  private RestClient tunnelServiceClient;
  private HashMap<String, String> headers;

  StreamUploadClient(Configuration conf, String projectName, String tableName)
    throws TunnelException {
    this.conf = conf;
    this.projectName = projectName;
    this.tableName = tableName;
    this.headers = new HashMap<String, String>();
    this.headers.put(Headers.CONTENT_LENGTH, "0");
    this.headers.put(HttpHeaders.HEADER_STREAM_VERSION, "1");

    initiate();
  }

  private void initiate() throws TunnelException {
    HashMap<String, String> param = new HashMap<String, String>();
    param.put("query", "meta");

    tunnelServiceClient = conf.newRestClient(projectName);
    Connection conn = null;
    try {
      conn = tunnelServiceClient.connect(getResource(), "GET", param, headers);
      Response resp = conn.getResponse();

      if (resp.isOK()) {
        loadFromJson(conn.getInputStream());
      } else {
        TunnelException ex = new TunnelException(conn.getInputStream());
        ex.setRequestId(resp.getHeader(HttpHeaders.HEADER_ODPS_REQUEST_ID));
        throw ex;
      }
    } catch (TunnelException e) {
      throw e;
    } catch (OdpsException e) {
      throw new TunnelException(e.getMessage(), e);
    } catch (IOException e) {
      throw new TunnelException(e.getMessage(), e);
    } finally {
      if (null != conn) {
        try {
          conn.disconnect();
        } catch (IOException ignored) {
        }
      }
    }
  }

  /**
   * 打开StreamUploadWriter
   */
  public StreamUploadWriter openStreamUploadWriter()
    throws TunnelException, IOException {
    HashMap<String, String> params = new HashMap<String, String>();
    HashMap<String, String> headers = new HashMap<String, String>(this.headers);
    headers.put(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
    headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
    return new StreamUploadWriter(tunnelServiceClient, getResource(), params, headers, schema);
  }

  public TableSchema getStreamSchema() { return this.schema; }

  private void loadFromJson(InputStream is)
    throws TunnelException {
    try {
      ObjectMapper mapper = JacksonParser.getObjectMapper();
      JsonNode tree = mapper.readTree(is);

      JsonNode node = tree.get("Schema");
      if (null != node && !node.isNull()) {
        schema = new TunnelTableSchema(node);
      } else {
        throw new TunnelException("Get table type failed");
      }

    } catch (Exception e) {
      throw new TunnelException("Invalid json content.", e);
    }
  }

  private String getResource() {
    return conf.getResource(projectName, tableName);
  }
}

