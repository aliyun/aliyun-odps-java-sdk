package com.aliyun.odps.tunnel.impl;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.HttpStatus;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TunnelTableSchema;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.FixedNettyChannelPool;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class UpsertSessionImpl extends SessionBase implements TableTunnel.UpsertSession {
    private long slotNum;
    private String status;
    private Map<Integer, Slot> buckets = new HashMap<>();
    private String hasher;
    private List<String> hashKeys = new ArrayList<>();
    private TunnelTableSchema recordSchema;
    private long commitTimeout;
    private long connectTimeout;
    private long readTimeout;
    private boolean supportPartialUpdate = false;

    // netty
    private EventLoopGroup group;
    private Bootstrap bootstrap;

    /**
     * scheduler to reload session periodically, which keep session alive.
     */
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();
    private FixedNettyChannelPool channelPool;

    public UpsertSessionImpl(UpsertSessionImpl.Builder builder) throws TunnelException, IOException {
        this.projectName = builder.getProjectName();
        this.schemaName = builder.getSchemaName();
        this.tableName = builder.getTableName();
        this.partitionSpec = builder.getPartitionSpec();
        this.config = builder.getConfig();
        this.httpClient = config.newRestClient(projectName);
        this.slotNum = builder.getSlotNum();
        this.commitTimeout = builder.getCommitTimeout();
        this.connectTimeout = config.getSocketConnectTimeout() * 1000L;
        this.readTimeout = config.getSocketTimeout() * 1000L;
        this.id = builder.getUpsertId();
        this.tunnelRetryHandler = new TunnelRetryHandler(config);
        if (id == null) {
            initiate();
        } else {
            reload(true);
        }
        initScheduler();
        if (builder.bootstrap == null) {
            initNettyBootstrap(builder.threadNum);
        } else {
            this.bootstrap = builder.bootstrap;
        }
        channelPool = newChannelPool(builder.concurrentNum);
    }


    public FixedNettyChannelPool getChannelPool() {
        return channelPool;
    }

    @Override
    public Record newRecord() {
        return new UpsertRecord(this.recordSchema.getColumns().toArray(new Column[0]));
    }

    @Override
    public UpsertStream.Builder buildUpsertStream() {
        return new UpsertStreamImpl.Builder().setSession(this)
            .setCompressOption(config.getCompressOption()).setListener(
                new DefaultUpsertSteamListener(this));
    }

    public static class DefaultUpsertSteamListener implements UpsertStream.Listener {
        UpsertSessionImpl session;

        public DefaultUpsertSteamListener(UpsertSessionImpl session) {
            this.session = session;
        }
        @Override
        public void onFlush(UpsertStream.FlushResult result) {
            // do nothing
        }

        @Override
        public boolean onFlushFail(Exception error, int retry) {
            if (error instanceof TunnelException) {
                Integer errorStatus = ((TunnelException) error).getStatus();
                if (errorStatus != null &&
                    (errorStatus == HttpStatus.BAD_GATEWAY
                     || errorStatus == HttpStatus.GATEWAY_TIMEOUT)) {
                    try {
                        session.reload(false);
                    } catch (TunnelException e) {
                        return false;
                    }
                }
            }
            return session.getTunnelRetryHandler().onFailure(error, retry);
        }
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
        reload(false);
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

    public boolean supportPartialUpdate() {
        return supportPartialUpdate;
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
                throw new TunnelException("Commit session failed, status:" + status);
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

    @Override
    public void close() {
        scheduler.shutdownNow();
        if (group != null) {
            group.shutdownGracefully().syncUninterruptibly();
        }
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

        HttpResult result = httpRequest(headers, params, "POST", "create upsert session");

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
                    if (tree.has(TunnelConstants.ENABLE_PARTIAL_UPDATE)) {
                        supportPartialUpdate = tree.get(TunnelConstants.ENABLE_PARTIAL_UPDATE).getAsBoolean();
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

  private void initScheduler() {
    final Runnable task = () -> {
      try {
        reload(false);
        if (!status.equalsIgnoreCase(TunnelConstants.SESSION_STATUS_NORMAL) &&
            !status.equalsIgnoreCase(TunnelConstants.SESSION_STATUS_COMMITTING)) {
          scheduler.shutdownNow();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    };
    scheduler.scheduleAtFixedRate(task, 30L, 30L, TimeUnit.SECONDS);
  }

    /**
     * 用于初始化http相关成员变量，包括netty的线程组和通用的header与parameter
     */
    private void initNettyBootstrap(int threadNum) throws TunnelException {
        group = new NioEventLoopGroup(threadNum);
        bootstrap = generateNettyBootstrap(config, group);
    }

    private FixedNettyChannelPool newChannelPool(int concurrentNum) throws TunnelException {
        try {
            URI uri = new URI(httpClient.getEndpoint() + getResource());
            String host = uri.getHost();
            int port = uri.getPort();
            if (port == -1) {
                if ("https".equalsIgnoreCase(uri.getScheme())) {
                    port = 443;
                } else {
                    port = 80;
                }
            }
            int finalPort = port;
            FixedNettyChannelPool.ChannelFactory channelFactory =
                () -> bootstrap.connect(host, finalPort).sync().channel();
            return new FixedNettyChannelPool(concurrentNum, channelFactory);
        } catch (Exception e) {
            throw new TunnelException(e.getMessage(), e);
        }
    }

    public static Bootstrap generateNettyBootstrap(Configuration configuration,
                                                   EventLoopGroup group) {
        Bootstrap bootstrap = new Bootstrap();
        Odps odps = configuration.getOdps();
        bootstrap.group(group).channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel channel) throws Exception {
                    URI uri = new URI(odps.getEndpoint());
                    if ("https".equalsIgnoreCase(uri.getScheme())) {
                        SslContextBuilder builder = SslContextBuilder.forClient();
                        if (odps.getRestClient().isIgnoreCerts()) {
                            builder = builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                        }
                        SslContext sc = builder.build();
                        channel.pipeline().addLast(sc.newHandler(channel.alloc()));
                    }
                    channel.pipeline().addLast(new HttpClientCodec())
                        .addLast(new HttpObjectAggregator(65536))
                        .addLast(new HttpContentDecompressor());
                }
            });
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                         configuration.getSocketConnectTimeout() * 1000);
        return bootstrap;
    }

    void updateBuckets(int bucketId, String newSlotServer) throws TunnelException {
        if (StringUtils.isNullOrEmpty(newSlotServer)) {
            reload(false);
        } else {
            try {
                readLock.lock();
                buckets.get(bucketId).setServer(newSlotServer);
            } finally {
                readLock.unlock();
            }
        }
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
        List<String> tags = config.getTags();
        if (tags != null) {
            headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
        }
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
            case ODPS_LZ4_FRAME: {
                headers.put(Headers.CONTENT_ENCODING, "x-lz4-frame");
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

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public long getReadTimeout() {
        return readTimeout;
    }

    public static class Builder implements TableTunnel.UpsertSession.Builder {
        private String upsertId;
        private String projectName;
        private String schemaName;
        private String tableName;
        private PartitionSpec partitionSpec;
        Bootstrap bootstrap;
        int concurrentNum = 20;
        int threadNum = 1;
        private long slotNum = 1;
        private long commitTimeout = 120 * 1000;
        Configuration config;

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

        public Configuration getConfig() {
            return config;
        }

        public UpsertSessionImpl.Builder setConfig(Configuration config) {
            if (config == null) {
                throw new IllegalArgumentException("config can not be null!");
            }
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

        @Override
        public UpsertSessionImpl.Builder setNetworkThreadNum(int threadNum) {
            this.threadNum = threadNum;
            return this;
        }

        @Override
        public UpsertSessionImpl.Builder setConcurrentNum(int concurrentNum) {
            this.concurrentNum = concurrentNum;
            return this;
        }

        @Override
        public UpsertSessionImpl.Builder setConnectTimeout(long timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("timeout value must be positive");
            }
            config.setSocketConnectTimeout((int) (timeout / 1000));
            return this;
        }

        @Override
        public UpsertSessionImpl.Builder setReadTimeout(long timeout) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("timeout value must be positive");
            }
            config.setSocketTimeout((int) (timeout / 1000));
            return this;
        }

        public UpsertSessionImpl.Builder setNettyBootStrap(Bootstrap bootstrap) {
            this.bootstrap = bootstrap;
            return this;
        }

        public UpsertSessionImpl build() throws TunnelException, IOException {
            return new UpsertSessionImpl(this);
        }
    }
}
