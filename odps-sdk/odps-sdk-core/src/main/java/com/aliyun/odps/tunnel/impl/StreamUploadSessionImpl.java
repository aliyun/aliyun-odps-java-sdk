package com.aliyun.odps.tunnel.impl;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;
import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_TUNNEL_METRICS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.HttpStatus;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.exceptions.SchemaMismatchException;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TunnelMetrics;
import com.aliyun.odps.tunnel.io.*;
import com.aliyun.odps.utils.ConnectionWatcher;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class StreamUploadSessionImpl extends StreamSessionBase implements TableTunnel.StreamUploadSession {
    public static class Builder extends TableTunnel.StreamUploadSession.Builder {
        private String projectName;
        private String tableName;
        private CompressOption compressOption = new CompressOption();
        private boolean p2pMode = false;
        private List<Column> zorderColumns;
        private Configuration config;
        private boolean dynamicPartition = false;

        public String getProjectName() {
            return projectName;
        }

        public StreamUploadSessionImpl.Builder setProjectName(String projectName) {
            this.projectName = projectName;
            return this;
        }

        public String getTableName() {
            return tableName;
        }

        public StreamUploadSessionImpl.Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public CompressOption getCompressOption() {
            return compressOption;
        }

        public StreamUploadSessionImpl.Builder setCompressOption(CompressOption compressOption) {
            this.compressOption = compressOption;
            return this;
        }

        public List<Column> getZorderColumns() {
            return zorderColumns;
        }

        public StreamUploadSessionImpl.Builder setZorderColumns(List<Column> zorderColumns) {
            this.zorderColumns = zorderColumns;
            return this;
        }

        public Configuration getConfig() {
            return config;
        }

        public StreamUploadSessionImpl.Builder setConfig(Configuration config) {
            this.config = config;
            return this;
        }

        public TableTunnel.StreamUploadSession build() throws TunnelException {
            return new StreamUploadSessionImpl(config,
                    projectName,
                    getSchemaName(),
                    tableName,
                    getPartitionSpec(),
                    isCreatePartition(),
                    getSlotNum(),
                    zorderColumns,
                    getSchemaVersion(),
                    allowSchemaMismatch,
                    dynamicPartition);
        }
    }

    protected StreamUploadSessionImpl.Slots slots;
    private boolean p2pMode = false;
    private List<Column> columns;
    private boolean checkLatestSchema;
    private boolean dynamicPartition = false;

    public StreamUploadSessionImpl(Configuration conf,
                                   String projectName,
                                   String schemaName,
                                   String tableName,
                                   String partitionSpec,
                                   boolean cretaPartition,
                                   long slotNum,
                                   List<Column> zorderColumns,
                                   String schemaVersion,
                                   boolean allowSchemaMismatch,
                                   boolean dynamicPartition) throws TunnelException {
        this.config = conf;
        this.projectName = projectName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.partitionSpec = partitionSpec;
        this.columns = zorderColumns;
        this.httpClient = Util.newRestClient(conf, projectName);
        this.schemaVersion = schemaVersion;
        this.checkLatestSchema = !allowSchemaMismatch;
        this.dynamicPartition = dynamicPartition;

        // Due to server-side architecture design, the latest TableSchema may not be used when creating a Session,
        // which may make users very confused in the scenario of not allowSchemaMismatch and use not specified schema version.
        // So the session is rebuilt here until the session schema is the latest table schema.
        if (this.checkLatestSchema && StringUtils.isNullOrEmpty(schemaVersion)) {
            initiateUntilUseLatestSchema(slotNum, cretaPartition);
        } else {
            initiate(slotNum, cretaPartition);
        }
    }

    private void initiateUntilUseLatestSchema(long slotNum, boolean createPartition)
        throws TunnelException {
        Table table = config.getOdps().tables().get(projectName, schemaName, tableName);
        // max retry 5 minutes
        int maxRetry = 60;
        for (int i = 0; i < maxRetry; i++) {
            schemaVersion = table.getSchemaVersion();
            if (i == maxRetry - 1) {
                throw new TunnelException(
                    "Cannot init session use latest schema version: " + schemaVersion);
            }
            try {
                initiate(slotNum, createPartition);
                break;
            } catch (OdpsException e) {
                if (!"NoSuchSchema".equals(e.getErrorCode())) {
                    throw e;
                }
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new TunnelException("Interrupt when initiate session.", e1);
                }
            }
        }
    }

    private void initiate(long slotNum, boolean createPartition) throws TunnelException {

        HashMap<String, String> params = getCommonParams();

        if (createPartition) {
            params.put(TunnelConstants.CREATE_PARTITION, "");
        }

        if (columns != null && columns.size() != 0) {
            params.put(TunnelConstants.ZORDER_COLUMNS, getColumnString());
        }

        if (schemaVersion != null && !schemaVersion.isEmpty()) {
            params.put(TunnelConstants.SCHEMA_VERSION, this.schemaVersion);
        }

        if (dynamicPartition) {
            params.put(TunnelConstants.PARAM_DYNAMIC_PARTITION, "true");
        }

        HashMap<String, String> headers = getCommonHeaders();
        if (slotNum > 0) {
            headers.put(HttpHeaders.HEADER_ODPS_SLOT_NUM, String.valueOf(slotNum));
        }

        StreamSessionBase.HttpResult result = httpRequest(headers, params, "POST", "create stream upload session");

        try {
            JsonObject tree = new JsonParser().parse(result.body).getAsJsonObject();
            this.slots = new Slots(loadFromJson(result.requestId, tree, false));
        } catch (JsonSyntaxException e) {
            throw new TunnelException(result.requestId, "Invalid json content: '" + result.body + "'", e);
        }
    }

    private void reload() throws TunnelException {

        HashMap<String, String> params = getCommonParams();

        params.put(TunnelConstants.UPLOADID, id);
        params.put(TunnelConstants.SCHEMA_VERSION, schemaVersion);

        HashMap<String, String> headers = getCommonHeaders();

        StreamSessionBase.HttpResult result = httpRequest(headers, params, "GET", "get stream upload session");

        try {
            JsonObject tree = new JsonParser().parse(result.body).getAsJsonObject();
            this.slots = new Slots(loadFromJson(result.requestId, tree, true));
        } catch (JsonSyntaxException e) {
            throw new TunnelException(result.requestId, "Invalid json content: '" + result.body + "'", e);
        }
    }

    public void reloadSlots(Slot slot, String server, int slotNum) throws TunnelException {
        if (slots.getSlotNum() != slotNum) {
            // reload slot routes if slot num changed
            reload();
        } else {
            // reset routed server slot rescheduled
            if (!slot.getServer().equals(server)) {
                slot.setServer(server);
            }
        }
    }

    static class Slots implements Iterable<Slot> {
        private Random rand = new Random();
        private final List<Slot> slots;
        private int curSlotIndex;
        private Iterator<Slot> iter;

        public Slots(List<Slot> slots) throws TunnelException {
            this.slots = slots;
            curSlotIndex = -1;

            if (this.slots.size() > 0) {
                curSlotIndex = rand.nextInt(this.slots.size());
            }

            // round robin iterator
            iter = new Iterator<Slot>() {
                @Override
                public boolean hasNext() {
                    return curSlotIndex >= 0;
                }

                @Override
                public synchronized Slot next() {
                    if (hasNext()) {
                        if (curSlotIndex >= slots.size()) {
                            curSlotIndex = 0;
                        }
                        return slots.get(curSlotIndex++);
                    } else {
                        return null;
                    }
                }
            };
        }

        @Override
        public Iterator<Slot> iterator() {
            return iter;
        }

        public int getSlotNum() {
            return slots.size();
        }
    }

    private Connection getConnection(CompressOption compress, Slot slot, long size, long reocrdCount)
            throws OdpsException, IOException {
        HashMap<String, String> params = new HashMap<String, String>();

        params.put(TunnelConstants.UPLOADID, id);
        params.put(TunnelConstants.SLOT_ID, slot.getSlot());
        params.put(TunnelConstants.SCHEMA_VERSION, schemaVersion);

        if (this.partitionSpec != null && this.partitionSpec.length() > 0) {
            params.put(TunnelConstants.RES_PARTITION, partitionSpec);
        }

        if (reocrdCount > 0) {
            params.put(TunnelConstants.RECORD_COUNT, String.valueOf(reocrdCount));
        }

        if (columns != null && columns.size() != 0) {
            params.put(TunnelConstants.ZORDER_COLUMNS, getColumnString());
        }

        if (dynamicPartition) {
            params.put(TunnelConstants.PARAM_DYNAMIC_PARTITION, "true");
        }

        HashMap<String, String> headers = getCommonHeaders();

        if (size < 0) {
            headers.put(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
        } else {
            headers.put(Headers.CONTENT_LENGTH, String.valueOf(size));
        }

        headers.put(Headers.CONTENT_TYPE, "application/octet-stream");

        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));

        headers.put(HttpHeaders.HEADER_ODPS_SLOT_NUM, String.valueOf(slots.getSlotNum()));

        if (!StringUtils.isNullOrEmpty(config.getQuotaName())) {
            params.put(TunnelConstants.PARAM_QUOTA_NAME, config.getQuotaName());
        }
        params.put(TunnelConstants.PARAM_CHECK_LATEST_SCHEMA, String.valueOf(checkLatestSchema));

        switch (compress.algorithm) {
            case ODPS_RAW: {
                break;
            }
            case ODPS_ZLIB: {
                headers.put(Headers.CONTENT_ENCODING, "deflate");
                break;
            }
            case ODPS_SNAPPY: {
                headers.put(Headers.CONTENT_ENCODING, "x-snappy-framed");
                break;
            }
            case ODPS_LZ4_FRAME: {
                headers.put(Headers.CONTENT_ENCODING, "x-lz4-frame");
                break;
            }
            default: {
                throw new TunnelException("unsupported compression option.");
            }
        }

        headers.put(HttpHeaders.HEADER_ODPS_ROUTED_SERVER, slot.getServer());

        if (p2pMode) {
            try {
                URI u = new URI(httpClient.getEndpoint());
                return httpClient.connect(getResource(), "PUT", params, headers,
                        u.getScheme() + "://" + slot.getIp());
            } catch (URISyntaxException e) {
                throw new TunnelException("Invalid endpoint: " + httpClient.getEndpoint());
            }
        } else {
            return httpClient.connect(getResource(), "PUT", params, headers);
        }
    }

    /**
     * 打开http链接，写入pack数据，然后关闭链
     *
     * @param pack
     *     pack数据
     */
    public String writeBlock(ProtobufRecordPack pack)
            throws IOException {
        return writeBlock(pack, 0);
    }

    /**
     * 打开http链接，写入pack数据，然后关闭链
     *
     * @param pack
     *     pack数据
     * @param timeout
     *     超时时间(单位毫秒)，0代表无超时。
     */
    public String writeBlock(ProtobufRecordPack pack, long timeout)
            throws IOException {
        TunnelRetryHandler tunnelRetryHandler = new TunnelRetryHandler(config);
        try {
            return tunnelRetryHandler.executeWithRetry(() -> {
                Connection conn = null;
                try {
                    Slot slot = slots.iterator().next();
                    conn =
                        getConnection(pack.getCompressOption(), slot, pack.getTotalBytes(),
                                      pack.getSize());
                    return sendBlock(pack, conn, slot, timeout);
                } finally {
                    if (conn != null) {
                        try {
                            conn.disconnect();
                        } catch (IOException e) {
                        }
                    }
                }
            }, errorCode -> {
                if (errorCode == HttpStatus.BAD_GATEWAY
                    || errorCode == HttpStatus.GATEWAY_TIMEOUT) {
                    try {
                        reload();
                    } catch (TunnelException e) {
                        throw new UncheckedIOException(new IOException(e.getMessage(), e));
                    }
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (RuntimeException | IOException re) {
            throw re;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private String sendBlock(ProtobufRecordPack pack, Connection conn, Slot slot, long timeout)
            throws IOException, TunnelException {
        if (null == conn) {
            throw new IOException("Invalid connection");
        }

        long startTime = System.currentTimeMillis();
        ByteArrayOutputStream baos = pack.getProtobufStream();
        if (timeout > 0) {
            ConnectionWatcher.getInstance().mark(conn, timeout);
        }
        Response response = null;
        try {
            baos.writeTo(conn.getOutputStream());
            conn.getOutputStream().close();
            baos.close();
            response = conn.getResponse();
        } catch (Throwable tr) {
            if (timeout > 0 && ConnectionWatcher.getInstance().checkTimedOut(conn)) {
                throw new SocketTimeoutException("Flush time exceeded timeout user set: " + timeout + "ms");
            }
            throw tr;
        } finally {
            if (timeout > 0) {
                ConnectionWatcher.getInstance().release(conn);
            }
        }
        if (!response.isOK()) {
            TunnelException exception = new TunnelException(response.getHeader(HEADER_ODPS_REQUEST_ID),
                                                      conn.getInputStream(),
                                                      response.getStatus());
            if (exception.getErrorCode().equals("SchemaModified") &&
                exception.getStatus().equals(412)) {
                throw new SchemaMismatchException("SchemaModified",
                                                  response.getHeader(
                                                      HttpHeaders.HEADER_ODPS_TUNNEL_LATEST_SCHEMA_VERSION));
            }
            throw exception;
        }

        reloadSlots(slot,
                    response.getHeader(HttpHeaders.HEADER_ODPS_ROUTED_SERVER),
                    Integer.valueOf(response.getHeader(HttpHeaders.HEADER_ODPS_SLOT_NUM)));

        String metricsStr = response.getHeader(HEADER_ODPS_TUNNEL_METRICS);
        TunnelMetrics batchMetrics =
            TunnelMetrics.parse(metricsStr, pack.getLocalWallTimeMs() + (System.currentTimeMillis() - startTime),
                                pack.getNetworkWallTimeMs() + (System.currentTimeMillis() - startTime));
        pack.addMetrics(batchMetrics);

        return response.getHeader(HEADER_ODPS_REQUEST_ID);
    }

    private String getColumnString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); ++i) {
            sb.append(columns.get(i).getName());
            if (i != columns.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setP2pMode(boolean mode) {
        this.p2pMode = mode;
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public String getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public String getQuotaName() {
        return quotaName;
    }

    @Override
    public TableTunnel.StreamRecordPack newRecordPack() throws IOException {
        if (dynamicPartition) {
            return new DynamicPartitionRecordPack(this, this.config.getCompressOption());
        } else {
            return new StreamRecordPackImpl(this, this.config.getCompressOption());
        }
    }

    @Override
    public TableTunnel.StreamRecordPack newRecordPack(CompressOption option) throws IOException {
        if (dynamicPartition) {
            return new DynamicPartitionRecordPack(this, option);
        } else {
            return new StreamRecordPackImpl(this, option);
        }
    }

    @Override
    public Record newRecord() {
        if (dynamicPartition) {
            return new PartitionRecord(schema.getColumns().toArray(new Column[0]));
        } else {
            return new ArrayRecord(schema.getColumns().toArray(new Column[0]));
        }
    }

    public void abort() throws TunnelException {
// do nothing
    }
}
