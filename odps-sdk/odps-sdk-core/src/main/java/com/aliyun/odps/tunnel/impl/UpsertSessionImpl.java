package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.*;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.gson.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UpsertSessionImpl extends SessionBase implements TableTunnel.UpsertSession {
    private long slotNum;
    private String status;
    private Map<Integer, Slot> buckets = new HashMap<>();
    private String hasher;
    private List<String> hashKeys = new ArrayList<>();
    private TunnelTableSchema recordSchema;
    private long commitTimeout;

    // netty
    private final EventLoopGroup group = new NioEventLoopGroup();
    private final Bootstrap bootstrap = new Bootstrap();

    // timer
    Timer timer = new Timer();
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();
    private Random random = new Random();

    public UpsertSessionImpl(UpsertSessionImpl.Builder builder) throws TunnelException, IOException {
        this.projectName = builder.getProjectName();
        this.schemaName = builder.getSchemaName();
        this.tableName = builder.getTableName();
        this.partitionSpec = builder.getPartitionSpec();
        this.config = builder.getConfig();
        this.httpClient = config.newRestClient(projectName);
        this.slotNum = builder.getSlotNum();
        this.commitTimeout = builder.getCommitTimeout();
        this.id = builder.getUpsertId();
        if (id == null) {
            initiate();
        } else {
            reload(true);
        }
        initTimer();
        initNetty();
    }

    @Override
    public Record newRecord() {
        return new UpsertRecord(this.recordSchema.getColumns().toArray(new Column[0]));
    }

    @Override
    public UpsertStream.Builder buildUpsertStream() {
        return new UpsertStreamImpl.Builder().setSession(this);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getQuotaName() {
        return quotaName;
    }

    @Override
    public String getStatus() throws TunnelException {
        if (timer == null) {
            reload(false);
        }
        try {
            readLock.lock();
            return status;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    public void commit(boolean async) throws TunnelException {
        HashMap<String, String> params = getCommonParams();
        params.put(TunnelConstants.UPSERT_ID, id);

        HashMap<String, String> headers = getCommonHeaders();
        headers.put(HttpHeaders.HEADER_ODPS_ROUTED_SERVER, buckets.get(0).getServer());

        HttpResult result = httpRequest(headers, params, "POST", "commit upsert session");

        load(result, false);

        if (!async) {
            int i = 1;
            long start = System.currentTimeMillis();
            while (status.equalsIgnoreCase(TunnelConstants.SESSION_STATUS_COMMITTING) ||
                   status.equalsIgnoreCase(TunnelConstants.SESSION_STATUS_NORMAL)) {
                try {
                    if (System.currentTimeMillis() - start > commitTimeout) {
                        throw new TunnelException("Commit session timeout");
                    }
                    Thread.sleep(i * 1000);
                    result = httpRequest(headers, params, "POST", "commit upsert session");
                    load(result, false);
                    if (i < 16) {
                        i = i * 2;
                    }
                } catch (InterruptedException e) {
                    throw new TunnelException(e.getMessage(), e);
                } catch (TunnelException e) {
                    if (e.getErrorCode().equalsIgnoreCase("UpsertSessionNotFound")) {
                        status = TunnelConstants.SESSION_STATUS_COMMITTED;
                    } else {
                        throw e;
                    }
                }
            }
            if (!status.equalsIgnoreCase(TunnelConstants.SESSION_STATUS_COMMITTED)) {
                throw new TunnelException("Commit session failed, status:" + getStatus());
            }
        }
    }

    public void abort() throws TunnelException {
        HashMap<String, String> params = getCommonParams();
        params.put(TunnelConstants.UPSERT_ID, id);

        HashMap<String, String> headers = getCommonHeaders();
        headers.put(HttpHeaders.HEADER_ODPS_ROUTED_SERVER, buckets.get(0).getServer());

        httpRequest(headers, params, "DELETE", "abort upsert session");
    }

    public void close() {
        timer.cancel();
        timer.purge();
        group.shutdownGracefully();
    }

    private void reload(boolean init) throws TunnelException {
        HashMap<String, String> params = getCommonParams();
        params.put(TunnelConstants.UPSERT_ID, id);

        HashMap<String, String> headers = getCommonHeaders();

        HttpResult result = httpRequest(headers, params, "GET", "get upsert session");

        load(result, init);
    }

    private void initiate() throws TunnelException, IOException {

        HashMap<String, String> params = getCommonParams();

        params.put(TunnelConstants.SLOT_NUM, String.valueOf(this.slotNum));

        HashMap<String, String> headers = getCommonHeaders();

        HttpResult result = httpRequest(headers, params, "POST", "create stream upload session");

        load(result, true);
    }

    private void load(HttpResult result, boolean loadAll) throws TunnelException {
        try {
            JsonObject tree = new JsonParser().parse(result.body).getAsJsonObject();
            loadFromJson(result.requestId, tree, loadAll);
        } catch (JsonSyntaxException e) {
            throw new TunnelException(result.requestId, "Invalid json content: '" + result.body + "'", e);
        }
    }

    private void loadFromJson(String requestId, JsonObject tree, boolean loadAll) throws TunnelException {
        try {
            if (tree.has("id") &&
                tree.has("schema") &&
                tree.has("hash_key") &&
                tree.has("hasher") &&
                tree.has("slots") &&
                tree.has("status")) {
                if (loadAll) {
                    // session id
                    id = tree.get("id").getAsString();
                    // schema
                    JsonObject tunnelTableSchema = tree.get("schema").getAsJsonObject();
                    schema = new TunnelTableSchema(tunnelTableSchema);
                    this.recordSchema = new TunnelTableSchema(tunnelTableSchema);
                    this.recordSchema.addColumn(
                            new Column(TunnelConstants.META_FIELD_VERSION, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)));
                    this.recordSchema.addColumn(
                            new Column(TunnelConstants.META_FIELD_APP_VERSION, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)));
                    this.recordSchema.addColumn(
                            new Column(TunnelConstants.META_FIELD_OPERATION, TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TINYINT)));
                    this.recordSchema.addColumn(new Column(TunnelConstants.META_FIELD_KEY_COLS, TypeInfoFactory
                            .getArrayTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT))));
                    this.recordSchema.addColumn(new Column(TunnelConstants.META_FIELD_VALUE_COLS, TypeInfoFactory
                            .getArrayTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT))));
                    // hash key
                    tree.get("hash_key").getAsJsonArray().forEach(v -> this.hashKeys.add(v.getAsString()));
                    // has function
                    this.hasher = tree.get("hasher").getAsString();

                    if (tree.has("quota_name")) {
                        quotaName = tree.get("quota_name").getAsString();
                    }
                }
            } else {
                throw new TunnelException(requestId, "Incomplete session info: '" + tree.toString() + "'");
            }

            // slots
            Map<Integer, Slot> bucketMap = new HashMap<>();
            List<Slot> slotList = new ArrayList<>();
            JsonArray slotElements = tree.getAsJsonArray("slots");
            for (JsonElement slotElement : slotElements) {
                JsonObject slotInfo = slotElement.getAsJsonObject();
                String slotId = slotInfo.get("slot_id").getAsString();
                JsonArray bucketElements = slotInfo.get("buckets").getAsJsonArray();
                String workerAddr = slotInfo.get("worker_addr").getAsString();
                Slot slot = new Slot(slotId, workerAddr);
                slotList.add(slot);
                for (JsonElement bucketElement : bucketElements) {
                    bucketMap.put(bucketElement.getAsInt(), slot);
                }
            }

            for (Integer bucket : bucketMap.keySet()) {
                if (bucket < 0 || bucket >= bucketMap.size()) {
                    throw new TunnelException("Invalid bucket value:" + bucket);
                }
            }

            try {
                writeLock.lock();
                this.buckets = bucketMap;
                this.status = tree.get("status").getAsString();
            } finally {
                writeLock.unlock();
            }
        } catch (TunnelException e) {
            throw e;
        } catch (Exception e) {
            throw new TunnelException(requestId, "Invalid json content: '" + tree.toString() + "'", e);
        }
    }

    private void initTimer() {
        timer.schedule(new TimerTask(){
            @Override
            public void run() {
                try {
                    reload(false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 30000L, 30000L);
    }

    /**
     * 用于初始化http相关成员变量，包括netty的线程组和通用的header与parameter
     */
    private void initNetty() {
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<Channel>() {
                    protected void initChannel(Channel channel) throws Exception {
                        URI uri = new URI(config.getOdps().getEndpoint());
                        if (uri.getScheme().equalsIgnoreCase("https")) {
                            SslContextBuilder builder = SslContextBuilder.forClient();
                            if (config.getOdps().getRestClient().isIgnoreCerts()) {
                                builder = builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                            }
                            SslContext sc = builder.build();
                            channel.pipeline().addLast(sc.newHandler(channel.alloc()));
                        }
                        channel.pipeline()
                                .addLast(new HttpClientCodec())
                                .addLast(new HttpObjectAggregator(65536))
                                .addLast(new HttpContentDecompressor());
                    }
                });
    }

    Request buildRequest(String method,
                                int bucket,
                                Slot slot,
                                long contentLength,
                                long recordCount,
                                CompressOption compressOption) throws TunnelException {
        if (slot.getServer().isEmpty()) {
            throw new TunnelException("slot addr is empty");
        }
        HashMap<String, String> params = getCommonParams();
        params.put(TunnelConstants.BUCKET_ID, String.valueOf(bucket));
        params.put(TunnelConstants.SLOT_ID, slot.getSlot());
        params.put(TunnelConstants.UPSERT_ID, id);

        HashMap<String, String> headers = Util.getCommonHeader();
        headers.put(Headers.CONTENT_LENGTH, String.valueOf(contentLength));
        headers.put(HttpHeaders.HEADER_ODPS_ROUTED_SERVER, slot.getServer());
        params.put(TunnelConstants.RECORD_COUNT, String.valueOf(recordCount));

        switch (compressOption.algorithm) {
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
            default: {
                throw new TunnelException("unsupported compression option.");
            }
        }

        return httpClient.buildRequest(getResource(), method, params, headers);
    }

    URI getEndpoint() throws TunnelException {
        return config.getEndpoint(projectName);
    }

    Map<Integer, Slot> getBuckets() {
        try {
            readLock.lock();
            return buckets;
        } finally {
            readLock.unlock();
        }
    }

    TunnelTableSchema getRecordSchema() {
        return recordSchema;
    }

    String getHasher() {
        return hasher;
    }

    List<Integer> getHashKeys() {
        List<Integer> keys = new ArrayList<>();
        for (String key : hashKeys) {
            keys.add(recordSchema.getColumnIndex(key));
        }
        return keys;
    }

    Bootstrap getBootstrap () {
        return bootstrap;
    }

    protected String getResource() {
        return config.getResource(projectName, schemaName, tableName)+ "/" + TunnelConstants.UPSERTS;
    }

    public static class Builder implements TableTunnel.UpsertSession.Builder {
        private String upsertId;
        private String projectName;
        private String schemaName;
        private String tableName;
        private PartitionSpec partitionSpec;
        private long slotNum = 1;
        private long commitTimeout = 120 * 1000;
        ConfigurationImpl config;

        public String getUpsertId() {
            return upsertId;
        }

        public UpsertSessionImpl.Builder setUpsertId(String upsertId) {
            this.upsertId = upsertId;
            return this;
        }

        public String getProjectName() {
            return projectName;
        }

        public UpsertSessionImpl.Builder setProjectName(String projectName) {
            this.projectName = projectName;
            return this;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public UpsertSessionImpl.Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public String getTableName() {
            return tableName;
        }

        public UpsertSessionImpl.Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public String getPartitionSpec() {
            return this.partitionSpec == null ? null : partitionSpec.toString().replaceAll("'", "");
        }

        public UpsertSessionImpl.Builder setPartitionSpec(PartitionSpec spec) {
            this.partitionSpec = spec;
            return this;
        }

        public UpsertSessionImpl.Builder setPartitionSpec(String spec) {
            this.partitionSpec = spec == null ? null : new PartitionSpec(spec);
            return this;
        }

        public long getSlotNum() {
            return slotNum;
        }

        public UpsertSessionImpl.Builder setSlotNum(long slotNum) {
            this.slotNum = slotNum;
            return this;
        }

        public ConfigurationImpl getConfig() {
            return config;
        }

        public UpsertSessionImpl.Builder setConfig(ConfigurationImpl config) {
            this.config = config;
            return this;
        }

        public long getCommitTimeout() {
            return commitTimeout;
        }

        public TableTunnel.UpsertSession.Builder setCommitTimeout(long commitTimeout) {
            if (commitTimeout <= 0) {
                throw new IllegalArgumentException("timeout value must be positive");
            }
            this.commitTimeout = commitTimeout;
            return this;
        }

        public UpsertSessionImpl build() throws TunnelException, IOException {
            return new UpsertSessionImpl(this);
        }
    }
}
