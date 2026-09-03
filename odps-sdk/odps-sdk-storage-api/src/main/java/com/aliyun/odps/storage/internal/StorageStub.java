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

package com.aliyun.odps.storage.internal;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.retry.RetryContext;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.MaxStorageException;
import com.aliyun.odps.storage.ServiceException;
import com.aliyun.odps.storage.internal.io.CrcStrippedInputStream;
import com.aliyun.odps.storage.internal.io.RawArrowRequestBody;
import com.aliyun.odps.storage.internal.models.BatchCompatibleCommitRequest;
import com.aliyun.odps.storage.internal.models.BatchCompatibleCreateSessionRequest;
import com.aliyun.odps.storage.internal.models.BatchCompatibleSessionResponse;
import com.aliyun.odps.storage.internal.models.BatchCompatibleWriteResponse;
import com.aliyun.odps.storage.internal.models.BlobWriteItem;
import com.aliyun.odps.storage.internal.models.BlobWriteRequest;
import com.aliyun.odps.storage.internal.models.BlobWriteResponse;
import com.aliyun.odps.storage.internal.models.CloseWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CloseWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.CreateInstanceReadSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateInstanceReadSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateInstanceReadStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateTableReadStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateTableWriteSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateTableWriteSessionResponse;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.GetTableWriteSessionResponse;
import com.aliyun.odps.storage.internal.models.TablePreviewRequest;
import com.aliyun.odps.storage.internal.models.GetWriteStreamRequest;
import com.aliyun.odps.storage.internal.models.GetWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.HttpResponse;
import com.aliyun.odps.storage.internal.models.ReadSchema;
import com.aliyun.odps.storage.internal.models.WriteSchema;
import com.aliyun.odps.storage.internal.models.WriteStreamResponse;
import com.aliyun.odps.storage.internal.retry.RetryHandler;
import com.aliyun.odps.storage.internal.serializer.ArrowOptionsSerializer;
import com.aliyun.odps.storage.internal.serializer.ReadSchemaDeserializer;
import com.aliyun.odps.storage.internal.serializer.WriteSchemaDeserializer;
import com.aliyun.odps.storage.internal.utils.IOUtils;
import com.aliyun.odps.storage.models.SplitMode;
import com.aliyun.odps.storage.models.TimestampUnit;
import com.aliyun.odps.storage.write.WriteMode;
import com.aliyun.odps.table.InstanceIdentifier;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithIndex;
import com.aliyun.odps.table.read.split.InputSplitWithRowRange;
import com.aliyun.odps.table.read.split.RowRange;
import com.aliyun.odps.utils.StringUtils;
import com.github.luben.zstd.ZstdOutputStream;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;

/**
 * The internal stub for communicating with the MaxCompute Storage V2 API endpoint.
 *
 * <p>This class is responsible for translating high-level SDK calls into
 * low-level HTTP requests, handling protocol-specific details like JSON
 * serialization and custom binary framing.
 *
 * <p>This is an internal component and is not intended for public use.
 *
 * <p>Example usage:
 * <pre>{@code
 * StubSettings stubSettings = StubSettings.newBuilder()
 *     .withEndpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
 *     .withCredentialProvider(credentialsProvider)
 *     .build();
 * StorageStub storageStub = new StorageStub(stubSettings);
 * // Use storageStub for API calls
 * storageStub.close();
 * }</pre>
 */
public class StorageStub implements Closeable {

  private static final String STORAGE_API_V2_RESOURCE = "api/storage/v3";
  private static final Logger log = LoggerFactory.getLogger(StorageStub.class);

  private final HttpClient httpClient;
  private final StubSettings settings;
  private final Gson gson;

  /**
   * Constructs a new StorageStub with the provided settings.
   *
   * @param stubSettings The settings for configuring the storage stub
   */
  public StorageStub(@NotNull StubSettings stubSettings) {
    this.httpClient = new HttpClient(stubSettings);
    this.settings = stubSettings;

    this.gson = new GsonBuilder()
      .disableHtmlEscaping()
      .registerTypeAdapter(TimestampUnit.class, new TimestampUnit.TimestampUnitSerializer())
      .registerTypeAdapter(TimestampUnit.class, new TimestampUnit.TimestampUnitDeserializer())
      .registerTypeAdapter(SplitMode.class, new SplitMode.SplitModeDeserializer())
      .registerTypeAdapter(SplitMode.class, new SplitMode.SplitModeSerializer())
      .registerTypeAdapter(ArrowOptions.class, new ArrowOptionsSerializer())
      .registerTypeAdapter(ReadSchema.class, new ReadSchemaDeserializer())
      .registerTypeAdapter(WriteSchema.class, new WriteSchemaDeserializer())
      .create();
  }

  // --- Session Management ---

  public CreateTableWriteSessionResponse createTableWriteSession(
    TableIdentifier tableId,
    CreateTableWriteSessionRequest request,
    WriteMode writeMode) {
    log.info("Creating table write session for table: {}", tableId);
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableCreateWriteSession");
    params.put("Target", resource);
    params.put("WriteMode", writeMode.getValue());

    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      buildCommonHeaders(),
      gson.toJson(request));

    CreateTableWriteSessionResponse
      createTableWriteSessionResponse =
      gson.fromJson(response.getBody(), CreateTableWriteSessionResponse.class);
    log.info("Successfully created table write session with ID: {}, requestId: {}",
             createTableWriteSessionResponse.getSessionId(),
             response.getRequestId());
    if (StringUtils.isNotBlank(createTableWriteSessionResponse.getWarningMessage())) {
      log.warn(createTableWriteSessionResponse.getWarningMessage());
    }
    createTableWriteSessionResponse.setRouteToken(
      response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER));

    return createTableWriteSessionResponse;
  }

  public GetTableWriteSessionResponse getTableWriteSession(
    TableIdentifier tableId, String sessionId, String routeToken, WriteMode writeMode) {
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableGetWriteSession");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    params.put("WriteMode", writeMode.getValue());

    Map<String, String> headers = buildCommonHeaders();
    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }
    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      "{}");

    GetTableWriteSessionResponse resp =
      gson.fromJson(response.getBody(), GetTableWriteSessionResponse.class);
    log.info("Successfully get table write session with ID: {}, requestId: {}",
             sessionId, response.getRequestId());
    if (StringUtils.isNotBlank(resp.getWarningMessage())) {
      log.warn(resp.getWarningMessage());
    }
    resp.setRouteToken(response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER));
    return resp;
  }

  public void commitTableWriteSession(TableIdentifier tableId, String sessionId,
                                      String routeToken, WriteMode writeMode) {
    commitTableWriteSession(tableId, sessionId, routeToken, null, null, writeMode);
  }

  /**
   * Commits a batch write session. Some tables (e.g. transactional / delta) require
   * {@code StreamIds} and matching {@code StreamVersions} in the JSON body; an empty body may
   * be rejected with 5xx.
   */
  public void commitTableWriteSession(TableIdentifier tableId, String sessionId,
                                      String routeToken,
                                      List<String> streamIds,
                                      List<Long> streamVersions,
                                      WriteMode writeMode) {
    log.info("Committing table write session for table: {} with session ID: {}", tableId, sessionId);
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableCommitWriteSession");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    params.put("WriteMode", writeMode.getValue());

    Map<String, String> headers = buildCommonHeaders();
    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }
    String body = "{}";
    if (streamIds != null && !streamIds.isEmpty() && streamVersions != null
        && streamVersions.size() == streamIds.size()) {
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("StreamIds", streamIds);
      payload.put("StreamVersions", streamVersions);
      body = gson.toJson(payload);
    }
    httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      body);
    log.info("Successfully committed table write session for table: {} with session ID: {}", tableId, sessionId);
  }

  public void abortTableWriteSession(TableIdentifier tableId, String sessionId, String routeToken,
                                     WriteMode writeMode) {
    log.info("Aborting table write session for table: {} with session ID: {}", tableId, sessionId);
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableAbortWriteSession");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    params.put("WriteMode", writeMode.getValue());

    Map<String, String> headers = buildCommonHeaders();
    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }

    httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      "{}");
    log.info("Successfully aborted table write session for table: {} with session ID: {}", tableId, sessionId);
  }

  // --- Batch-compatible block protocol ---

  public BatchCompatibleSessionResponse createBatchCompatibleSession(
      TableIdentifier tableId,
      BatchCompatibleCreateSessionRequest request) {
    Map<String, String> params = buildBatchCompatibleSessionParams(
        tableId, "TableCreateWriteSession", null);
    params.put("enableQuotaToken", "true");
    HttpResponse response = httpClient.request(
        STORAGE_API_V2_RESOURCE,
        "POST",
        params,
        buildCommonHeaders(),
        gson.toJson(request));

    BatchCompatibleSessionResponse result =
        gson.fromJson(response.getBody(), BatchCompatibleSessionResponse.class);
    if (result == null) {
      throw new ClientException("Create batch-compatible session returned an empty response");
    }
    result.setRouteToken(response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER));
    return result;
  }

  public BatchCompatibleSessionResponse getBatchCompatibleSession(
      TableIdentifier tableId,
      String sessionId,
      String routeToken) {
    Map<String, String> params = buildBatchCompatibleSessionParams(
        tableId, "TableGetWriteSession", sessionId);
    HttpResponse response = httpClient.request(
        STORAGE_API_V2_RESOURCE,
        "POST",
        params,
        buildRouteHeaders(routeToken),
        "{}");

    BatchCompatibleSessionResponse result =
        gson.fromJson(response.getBody(), BatchCompatibleSessionResponse.class);
    if (result == null) {
      throw new ClientException("Get batch-compatible session returned an empty response");
    }
    result.setRouteToken(response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER));
    return result;
  }

  public BatchCompatibleWriteResponse writeBatchCompatibleBlock(
      TableIdentifier tableId,
      String sessionId,
      int blockNumber,
      int attemptNumber,
      RequestBody arrowStreamBody,
      String routeToken,
      String quotaToken) {
    Map<String, String> params = buildBatchCompatibleSessionParams(
        tableId, "TableWrite", sessionId);
    params.put("BlockNumber", String.valueOf(blockNumber));
    params.put("AttemptNumber", String.valueOf(attemptNumber));
    params.put("quotaToken", quotaToken);

    try {
      RetryHandler retryHandler = settings.getRetryHandler();
      if (retryHandler == null) {
        retryHandler = new RetryHandler();
      }
      RetryHandler effectiveRetryHandler = retryHandler;
      HttpResponse response = effectiveRetryHandler.executeWithRetry(context ->
          httpClient.streamUpload(
              STORAGE_API_V2_RESOURCE,
              "POST",
              params,
              buildRouteHeaders(routeToken),
              arrowStreamBody,
              context));
      return gson.fromJson(response.getBody(), BatchCompatibleWriteResponse.class);
    } catch (ServiceException | ClientException e) {
      throw e;
    } catch (Exception e) {
      throw new ClientException(e);
    }
  }

  public BatchCompatibleSessionResponse commitBatchCompatibleSession(
      TableIdentifier tableId,
      String sessionId,
      String routeToken,
      Collection<String> commitMessages) {
    Map<String, String> params = buildBatchCompatibleSessionParams(
        tableId, "TableCommitWriteSession", sessionId);
    HttpResponse response = httpClient.request(
        STORAGE_API_V2_RESOURCE,
        "POST",
        params,
        buildRouteHeaders(routeToken),
        gson.toJson(new BatchCompatibleCommitRequest(commitMessages)));

    BatchCompatibleSessionResponse result =
        gson.fromJson(response.getBody(), BatchCompatibleSessionResponse.class);
    if (result == null) {
      throw new ClientException("Commit batch-compatible session returned an empty response");
    }
    result.setRouteToken(response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER));
    return result;
  }

  private Map<String, String> buildBatchCompatibleSessionParams(
      TableIdentifier tableId,
      String action,
      String sessionId) {
    String resource = String.format(
        "projects.%s.schemas.%s.tables.%s",
        tableId.getProject(),
        tableId.getSchema(),
        tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", action);
    params.put("Target", resource);
    params.put("WriteMode", WriteMode.BATCH_COMPATIBLE.getValue());
    if (sessionId != null) {
      params.put("SessionId", sessionId);
    }
    return params;
  }

  private Map<String, String> buildRouteHeaders(String routeToken) {
    Map<String, String> headers = buildCommonHeaders();
    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }
    return headers;
  }

  public CreateWriteStreamResponse createTableWriteStream(
    TableIdentifier tableId,
    String sessionId,
    CreateWriteStreamRequest request,
    String routeToken,
    WriteMode writeMode) {
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableCreateWriteStream");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    params.put("WriteMode", writeMode.getValue());

    Map<String, String> headers = buildCommonHeaders();
    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }

    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      gson.toJson(request));

    log.info(
      "Successfully created table write stream with Table: {}, session ID: {}, stream ID:{}, request ID: {}",
      tableId,
      sessionId,
      request.getStreamId(),
      response.getRequestId()
    );

    CreateWriteStreamResponse createWriteStreamResponse = gson.fromJson(response.getBody(), CreateWriteStreamResponse.class);
    routeToken = response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER);
    if (routeToken != null) {
      createWriteStreamResponse.setRouteToken(routeToken);
    }
    return createWriteStreamResponse;
  }

  public GetWriteStreamResponse getWriteStream(
    GetWriteStreamRequest request, String routeToken, WriteMode writeMode) {
    TableIdentifier tableId = request.getTableIdentifier();
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableGetWriteStream");
    params.put("Target", resource);
    params.put("SessionId", request.getSessionId());
    params.put("StreamId", request.getStreamId());
    params.put("StreamVersion", String.valueOf(request.getStreamVersion()));
    params.put("WriteMode", writeMode.getValue());

    if (Boolean.TRUE.equals(request.getExactlyOnceMode())) {
      params.put("ExactlyOnceMode", "true");
    }

    Map<String, String> headers = buildCommonHeaders();
    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }

    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      gson.toJson(request));

    return gson.fromJson(response.getBody(),
                         GetWriteStreamResponse.class);
  }

  /**
   * @deprecated Use {@link #writeTable(TableIdentifier, String, String, long, long, RequestBody, String, String, Long, long, String)} instead.
   */
  @Deprecated
  public HttpResponse writeTable(TableIdentifier tableId, String sessionId, String streamId,
                         long streamVersion,
                         long recordCount,
                         RequestBody arrowStreamBody,
                         String routeToken,
                         WriteMode writeMode) {
    return writeTable(tableId, sessionId, streamId, streamVersion, recordCount, arrowStreamBody, routeToken, null, null, writeMode);
  }

  public HttpResponse writeTable(TableIdentifier tableId, String sessionId, String streamId,
                         long streamVersion,
                         long recordCount,
                         RequestBody arrowStreamBody,
                         String routeToken,
                         String streamingTableId,
                         Long streamingSchemaVersion,
                         WriteMode writeMode) {
    return writeTable(tableId, sessionId, streamId, streamVersion, recordCount, arrowStreamBody,
                      routeToken, streamingTableId, streamingSchemaVersion, -1, null, writeMode);
  }

  public HttpResponse writeTable(TableIdentifier tableId, String sessionId, String streamId,
                         long streamVersion,
                         long recordCount,
                         RequestBody arrowStreamBody,
                         String routeToken,
                         String streamingTableId,
                         Long streamingSchemaVersion,
                         long rowOffset,
                         String accessToken,
                         WriteMode writeMode) {
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableWrite");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    params.put("StreamId", streamId);
    params.put("StreamVersion", String.valueOf(streamVersion));
    params.put("Count", String.valueOf(recordCount));
    params.put("WriteMode", writeMode.getValue());

    if (streamingTableId != null) {
      params.put("TableId", streamingTableId);
    }
    if (streamingSchemaVersion != null) {
      params.put("SchemaVersion", String.valueOf(streamingSchemaVersion));
    }
    if (rowOffset >= 0) {
      params.put("RowOffset", String.valueOf(rowOffset));
    }

    Map<String, String> headers = buildCommonHeaders();
    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }
    if (StringUtils.isNotBlank(accessToken)) {
      headers.put(Constants.WRITE_ACCESS_TOKEN_HEADER, accessToken);
    }

    try {
      RetryHandler retryHandler = settings.getRetryHandler();
      if (retryHandler == null) {
        retryHandler = new RetryHandler();
      }
      long startTime = System.currentTimeMillis();
      HttpResponse httpResponse = retryHandler.executeWithRetry(context -> {
        return httpClient.streamUpload(
          STORAGE_API_V2_RESOURCE,
          "POST",
          params,
          headers,
          arrowStreamBody,
          context);
      });
      log.info(
        "Successfully write {} records, {} bytes(compressed) to table: {}, cost {}ms, request ID: {}", recordCount,
        ((RawArrowRequestBody)arrowStreamBody).getTotalBytes(), tableId, System.currentTimeMillis() - startTime,
        httpResponse.getRequestId());
      return httpResponse;
    } catch (Exception e) {
      if (e instanceof ClientException) {
        throw (ClientException) e;
      } else if (e instanceof ServiceException) {
        throw (ServiceException) e;
      } else {
        throw new ClientException(e);
      }
    }
  }

  /**
   * Parses the response body from a write operation into WriteStreamResponse.
   * Used in Exactly-Once mode to extract ExactlyOnceRowOffset.
   *
   * @param httpResponse the HTTP response from writeTable
   * @return the parsed WriteStreamResponse
   */
  public WriteStreamResponse parseWriteStreamResponse(HttpResponse httpResponse) {
    return gson.fromJson(httpResponse.getBody(), WriteStreamResponse.class);
  }

  public CloseWriteStreamResponse closeWriteStream(TableIdentifier tableId,
                                                   CloseWriteStreamRequest request,
                                                   String routeToken,
                                                   WriteMode writeMode) {
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableCloseWriteStream");
    params.put("Target", resource);
    params.put("SessionId", request.getSessionId());
    params.put("WriteMode", writeMode.getValue());

    Map<String, String> headers = buildCommonHeaders();
    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }

    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      gson.toJson(request));

    log.info(
      "Close write stream for table {} success, Session ID: {}, Stream ID: {}, request ID: {}",
      tableId, request.getSessionId(), request.getStreamId(), response.getRequestId());
    return gson.fromJson(response.getBody(), CloseWriteStreamResponse.class);
  }

  // --- Read Session Management ---

  public CreateTableReadSessionResponse createTableReadSession(
    TableIdentifier tableId, CreateTableReadSessionRequest request) {
    log.info("Creating table read session for table: {}", tableId);
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableCreateReadSession");
    params.put("Target", resource);

    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      buildCommonHeaders(),
      gson.toJson(request));

    CreateTableReadSessionResponse
      createTableReadSessionResponse =
      gson.fromJson(response.getBody(), CreateTableReadSessionResponse.class);
    createTableReadSessionResponse.setRouteToken(response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER));

    log.info("Successfully created table read session with ID: {}", createTableReadSessionResponse.getSessionId());

    return createTableReadSessionResponse;
  }

  public CreateTableReadSessionResponse getTableReadSession(
    TableIdentifier tableId, String sessionId, boolean sessionRefresh) {
    log.info("Get table read session {} for table: {}, sessionRefresh {}", sessionId, tableId, sessionRefresh);
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());

    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableGetReadSession");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    params.put("session_refresh", String.valueOf(sessionRefresh));

    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      buildCommonHeaders(),
      "{}");

    CreateTableReadSessionResponse
      getTableReadSessionResponse =
      gson.fromJson(response.getBody(), CreateTableReadSessionResponse.class);
    getTableReadSessionResponse.setRouteToken(response.getFirstHeader(Constants.ROUTE_TOKEN_HEADER));

    log.info("Successfully get table read session with ID: {}", getTableReadSessionResponse.getSessionId());
    return getTableReadSessionResponse;
  }

  // --- Stream Management ---

  public InputStream createTableReadStream(TableIdentifier tableId,
                                           InputSplit split,
                                           CreateTableReadStreamRequest request,
                                           String routeToken) {
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableRead");
    params.put("Target", resource);
    params.put("SessionId", split.getSessionId());

    if (split instanceof InputSplitWithIndex) {
      params.put("Index", String.valueOf(((InputSplitWithIndex) split).getSplitIndex()));
    } else if (split instanceof InputSplitWithRowRange) {
      RowRange rowRange = ((InputSplitWithRowRange) split).getRowRange();
      params.put("Offset", String.valueOf(rowRange.getStartIndex()));
      params.put("Count", String.valueOf(rowRange.getNumRecord()));
    }

    Map<String, String> headers = buildCommonHeaders();
    headers.put("ACCEPT-ENCODING", "x-lz4-frame");

    if (StringUtils.isNotBlank(routeToken)) {
      headers.put(Constants.ROUTE_TOKEN_HEADER, routeToken);
    }

    HttpResponse response = httpClient.streamDownload(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      gson.toJson(request));

    log.info("Successfully created read stream for table: {} with session ID: {}, request ID: {}",
             tableId,
             split.getSessionId(), response.getRequestId());
    return response.getInputStream();
  }

  public InputStream preview(TableIdentifier tableId, String partitionSpec,
                             List<String> columns, Integer limit) {
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(), tableId.getSchema(),
                    tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TablePreview");
    params.put("Target", resource);

    if (limit != null) {
      params.put("Limit", String.valueOf(limit));
    }

    if (partitionSpec != null) {
      params.put("Partition", partitionSpec);
    }

    TablePreviewRequest request = new TablePreviewRequest();
    if (limit != null) {
      request.setLimit(limit);
    }
    if (partitionSpec != null) {
      request.setPartition(partitionSpec);
    }
    if (columns != null) {
      request.setColumns(columns);
    }

    HttpResponse response = httpClient.streamDownload(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      buildCommonHeaders(),
      gson.toJson(request));

    log.info("Successfully preview table: {}, request ID: {}", tableId,
             response.getRequestId());

    return response.getInputStream();
  }

  // --- Instance Session Management ---
  public CreateInstanceReadSessionResponse createInstanceReadSession(InstanceIdentifier instanceId,
                                                                     CreateInstanceReadSessionRequest request) {
    String resource =
      String.format("projects.%s.instances.%s", instanceId.getProject(), instanceId.getInstanceId());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "InstanceCreateReadSession");
    params.put("Target", resource);


    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      buildCommonHeaders(),
      gson.toJson(request));

    return gson.fromJson(response.getBody(), CreateInstanceReadSessionResponse.class);
  }

  public CreateInstanceReadSessionResponse getInstanceReadSession(
    InstanceIdentifier instanceId, String sessionId) {
    String resource =
      String.format("projects.%s.instances.%s", instanceId.getProject(), instanceId.getInstanceId());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "InstanceGetReadSession");
    params.put("Target", resource);
    params.put("SessionId", sessionId);

    HttpResponse response = httpClient.request(
      STORAGE_API_V2_RESOURCE,
      "GET",
      params,
      buildCommonHeaders(),
      null);

    return gson.fromJson(response.getBody(), CreateInstanceReadSessionResponse.class);
  }

  public InputStream createInstanceReadStream(InstanceIdentifier instanceId,
                                              String sessionId,
                                              Long count,
                                              Long offset,
                                              CreateInstanceReadStreamRequest request) {
    String resource =
      String.format("projects.%s.instances.%s", instanceId.getProject(),
                    instanceId.getInstanceId());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "InstanceRead");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    if (count != null) {
      params.put("Count", String.valueOf(count));
    }
    if (offset != null) {
      params.put("Offset", String.valueOf(offset));
    }

    Map<String, String> headers = buildCommonHeaders();
    headers.put("ACCEPT-ENCODING", "x-lz4-frame");

    HttpResponse response = httpClient.streamDownload(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      gson.toJson(request));

    log.info(
      "Successfully created read stream for instance: {} with session ID: {}, request ID: {}",
      instanceId, sessionId, response.getRequestId());
    return response.getInputStream();
  }

  public BlobWriteResponse tableBatchWriteBlob(TableIdentifier tableId,
                                                String sessionId,
                                                String streamId,
                                                long streamVersion,
                                                List<BlobWriteItem> blobs) {
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(),
                    tableId.getSchema(), tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableWriteBlob");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    params.put("StreamId", streamId);
    params.put("StreamVersion", String.valueOf(streamVersion));

    params.put("Mode", "Batch");

    Map<String, String> headers = buildCommonHeaders();
    //headers.put("Content-Encoding", "zstd");
    MaxStorageException lastException = null;
    RetryContext retryContext = RetryContext.create();

    try {
      // 1. Serialize all blob items into a single byte array.
      // This is our complete, uncompressed data source, held in memory.
      final byte[] uncompressedBody = BlobWriteItem.writeBlobs(blobs);

      for (int attempt = 0; attempt <= 3; attempt++) {
        if (attempt > 0) {
          log.info("Retrying batch blob upload. Attempt {}/{}", attempt, 3);
          // No stream to reset, we just create a new one from the byte array.
        }

        // 2. For each attempt, create a fresh stream from the byte array.
        // This is cheap and effectively serves the same purpose as reset().
        InputStream dataStream = new ByteArrayInputStream(uncompressedBody);
        RequestBody requestBody = createStreamingBody(dataStream, CompressionCodec.NO_COMPRESSION);

        try {
          // 3. Make the HTTP call.
          HttpResponse response =
            httpClient.streamUpload(
                STORAGE_API_V2_RESOURCE, "POST", params, headers, requestBody, retryContext);

          // 4. Parse the response.
          BlobWriteResponse blobWriteResponse =
            gson.fromJson(response.getBody(), BlobWriteResponse.class);

          if (StringUtils.isNotBlank(blobWriteResponse.getWarningMessage())) {
            log.warn("Warning from batch blob write: {}", blobWriteResponse.getWarningMessage());
          }

          // 5. Log success information.
          log.info(
            "Uploaded a batch of {} blobs to table {} successfully. Total uncompressed size: {}, request ID: {}",
            blobs.size(),
            tableId,
            blobWriteResponse.getSize(),
            response.getRequestId());

          if (blobWriteResponse.getBlobReferences() != null) {
            log.debug("Received {} blob references.", blobWriteResponse.getBlobReferences().size());
          }

          return blobWriteResponse;

        } catch (MaxStorageException e) {
          lastException = e;
          log.error("Batch upload attempt {} failed due to network or server error: {}", attempt, e.getMessage(), e);
          retryContext = retryContext.next();
        }
      }
      // If all retries fail, throw the last captured exception.
      throw lastException;

    } catch (IOException e) {
      // This exception would come from BlobWriteItem.writeBlobs().
      throw new ClientException("Failed to serialize blob data for batch upload.", e);
    }
  }

  public BlobWriteResponse tableWriteBlob(TableIdentifier tableId,
                                          String sessionId,
                                          String streamId,
                                          long streamVersion,
                                          PartitionSpec partitionValues,
                                          long columnId,
                                          InputStream data) {
    String resource =
      String.format("projects.%s.schemas.%s.tables.%s", tableId.getProject(),
                    tableId.getSchema(), tableId.getTable());
    Map<String, String> params = new HashMap<>();
    params.put("Action", "TableWriteBlob");
    params.put("Target", resource);
    params.put("SessionId", sessionId);
    params.put("StreamId", streamId);
    params.put("StreamVersion", String.valueOf(streamVersion));
    params.put("PartitionValues",
               partitionValues == null ? "" : partitionValues.toString(false, true));
    params.put("ColumnIndex", String.valueOf(columnId));

    Map<String, String> headers = buildCommonHeaders();
    headers.put("Content-Encoding", "zstd");
    MaxStorageException lastException = null;
    RetryContext retryContext = RetryContext.create();
    try (InputStream repeatableStream = IOUtils.newRepeatableInputStream(data)) {
      for (int attempt = 0; attempt <= 3; attempt++) {
        if (attempt > 0) {
          try {
            log.info("Retrying upload. Attempt {}/" + 3, attempt);
            repeatableStream.reset();
          } catch (IOException e) {
            throw new ClientException("Failed to reset stream for retry.", lastException);
          }
        }
        RequestBody requestBody = createStreamingBody(repeatableStream, CompressionCodec.ZSTD);
        try {
          HttpResponse response =
            httpClient.streamUpload(
                STORAGE_API_V2_RESOURCE, "POST", params, headers, requestBody, retryContext);
          BlobWriteResponse
            blobWriteResponse =
            gson.fromJson(response.getBody(), BlobWriteResponse.class);
          if (StringUtils.isNotBlank(blobWriteResponse.getWarningMessage())) {
            log.warn("Warning: {}", blobWriteResponse.getWarningMessage());
          }
          log.info(
            "Uploaded blob to table {} column {} success with reference {}, size {}, request ID: {}",
            tableId, columnId,
            blobWriteResponse.getBlobReference(),
            blobWriteResponse.getSize(),
            response.getRequestId());
          return blobWriteResponse;
        } catch (MaxStorageException e) {
          lastException = e;
          log.error("Upload attempt {} failed due to network error: {}", attempt, e.getMessage(),
                    e);
          retryContext = retryContext.next();
        }
      }
      throw lastException;
    } catch (IOException e) {
      throw new ClientException("Failed to read blob input stream.", e);
    }
  }

  public InputStream readBlobs(List<String> blobRefs) {
    Map<String, String> params = new HashMap<>();
    params.put("Action", "BlobRead");
    params.put("Target", "generic.blob");

    Map<String, String> headers = buildCommonHeaders();
    headers.put("ACCEPT-ENCODING", "x-lz4-frame");

    BlobWriteRequest request = new BlobWriteRequest();
    request.setBlobReferences(blobRefs);

    HttpResponse response = httpClient.streamDownload(
      STORAGE_API_V2_RESOURCE,
      "POST",
      params,
      headers,
      gson.toJson(request));

    log.info("Successfully open read blob stream, request ID: {}",
      response.getRequestId());
    try {
      return new LZ4FrameInputStream(new CrcStrippedInputStream(response.getInputStream()));
    } catch (Exception e) {
      throw new ClientException(e.getMessage(), e);
    }
  }

  // --- Helper Methods ---

  private RequestBody createStreamingBody(final InputStream inputStream, CompressionCodec compressionCodec) {

    return new RequestBody() {
      @Override
      public MediaType contentType() {
        return HttpClient.OCTET_STREAM;
      }

      @Override
      public long contentLength() {
        return -1L;
      }

      @Override
      public void writeTo(BufferedSink bufferedSink) throws IOException {
        OutputStream compressingStream = null;
        try {
          OutputStream sinkOutputStream = bufferedSink.outputStream();
          compressingStream = wrapForCompression(sinkOutputStream, compressionCodec);
          IOUtils.transferTo(inputStream, compressingStream);
          compressingStream.flush();
        } finally {
          if (compressingStream != null) {
            try {
              compressingStream.close();
            } catch (IOException e) {
              log.warn("Failed to close compressing stream in finally block.", e);
            }
          }
          try {
            inputStream.close();
          } catch (IOException e) {
            log.warn("Failed to close input stream in finally block.", e);
          }
        }
      }
    };
  }

  /**
   * Wraps a target OutputStream with a compression layer.
   * Data written to the returned stream will be compressed before being
   * written to the original target stream.
   *
   * @param targetStream The final destination for the compressed data.
   * @param compressionCodec The compression algorithm to use.
   * @return A new OutputStream that performs compression.
   * @throws IOException If an I/O error occurs.
   */
  public OutputStream wrapForCompression(OutputStream targetStream, CompressionCodec compressionCodec) throws IOException {
    if (targetStream == null || compressionCodec == null) {
      return targetStream;
    }

    switch (compressionCodec) {
      case LZ4_FRAME:
        return new LZ4FrameOutputStream(targetStream);
      case ZSTD:
        return new ZstdOutputStream(targetStream);
      case NO_COMPRESSION:
      default:
        return targetStream;
    }
  }

  private Map<String, String> buildCommonHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json; charset=utf-8");
    //headers.put(Headers.ODPS_NAMESPACE_ID, "default");
    return headers;
  }

  @Override
  /**
   * Closes the storage stub and releases all associated resources.
   *
   * <p>This method shuts down the underlying HTTP client and should be called
   * when the storage stub is no longer needed to prevent resource leaks.
   */
  public void close() {
    this.httpClient.shutdown();
  }
}
