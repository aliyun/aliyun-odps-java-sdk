package com.aliyun.odps.tunnel;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordPack;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.*;
import com.aliyun.odps.utils.ConnectionWatcher;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

public class StreamUploadSessionImpl implements TableTunnel.StreamUploadSession {
    private String id;
    private Configuration conf;
    private TableSchema schema = new TableSchema();
    private String projectName;
    private String tableName;
    private String partitionSpec;
    private RestClient tunnelServiceClient;
    private Slots slots;
    private boolean p2pMode = false;
    private List<Column> columns;

    StreamUploadSessionImpl(String projectName, String tableName, String partitionSpec, Configuration config, long slotNum, boolean createPartition, List<Column> columns) throws TunnelException {
        this.conf = config;
        this.projectName = projectName;
        this.tableName = tableName;
        this.partitionSpec = partitionSpec;
        this.columns = columns;

        tunnelServiceClient = conf.newRestClient(projectName);
        initiate(slotNum, createPartition);
    }

    private void initiate(long slotNum, boolean createPartition) throws TunnelException {

        HashMap<String, String> params = new HashMap<String, String>();

        if (this.partitionSpec != null && this.partitionSpec.length() > 0) {
            params.put(TunnelConstants.RES_PARTITION, partitionSpec);
        }

        if (createPartition) {
            params.put(TunnelConstants.CREATE_PARTITION, "");
        }

        if (columns != null && columns.size() != 0) {
            params.put(TunnelConstants.ZORDER_COLUMNS, getColumnString());
        }

        HashMap<String, String> headers = TableTunnel.getCommonHeader();

        if (slotNum > 0) {
            headers.put(HttpHeaders.HEADER_ODPS_SLOT_NUM, String.valueOf(slotNum));
        }

        String requestId = null;
        Connection conn = null;
        try {
            conn = tunnelServiceClient.connect(getResource(), "POST", params, headers);
            Response resp = conn.getResponse();
            requestId = resp.getHeader(HEADER_ODPS_REQUEST_ID);

            if (resp.isOK()) {
                loadFromJson(requestId, conn.getInputStream(), false);

            } else {
                throw new TunnelException(requestId, conn.getInputStream(), resp.getStatus());
            }
        } catch (IOException e) {
            throw new TunnelException(requestId, "Failed to create upload session with tunnel endpoint "
                    + tunnelServiceClient.getEndpoint(), e);
        } catch (TunnelException e) {
            // Do not delete here! TunnelException extends from OdpsException.
            throw e;
        } catch (OdpsException e) {
            throw new TunnelException(requestId, e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (IOException e) {
                }
            }
        }
    }

    private void reload(String server) throws TunnelException {

        HashMap<String, String> params = new HashMap<String, String>();

        params.put(TunnelConstants.UPLOADID, id);

        if (this.partitionSpec != null && this.partitionSpec.length() > 0) {
            params.put(TunnelConstants.RES_PARTITION, partitionSpec);
        }

        HashMap<String, String> headers = TableTunnel.getCommonHeader();

        Connection conn = null;
        String requestId = null;
        try {
            conn = tunnelServiceClient.connect(getResource(), "GET", params, headers);
            Response resp = conn.getResponse();
            requestId = resp.getHeader(HEADER_ODPS_REQUEST_ID);

            if (resp.isOK()) {
                loadFromJson(requestId, conn.getInputStream(), true);

            } else {
                throw new TunnelException(requestId, conn.getInputStream(), resp.getStatus());
            }
        } catch (IOException e) {
            throw new TunnelException(requestId, "Failed to reload upload session with tunnel endpoint "
                    + tunnelServiceClient.getEndpoint(), e);
        } catch (TunnelException e) {
            // Do not delete here! TunnelException extends from OdpsException.
            throw e;
        } catch (OdpsException e) {
            throw new TunnelException(requestId, e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (IOException e) {
                }
            }
        }
    }

    private void loadFromJson(String requestId, InputStream is, boolean reload) throws TunnelException {
        String json = "";
        try {
            json = IOUtils.readStreamAsString(is);
            JsonObject tree = new JsonParser().parse(json).getAsJsonObject();

            if (!reload) {
                if (tree.has("session_name") && tree.has("schema")) {
                    // session id
                    id = tree.get("session_name").getAsString();
                    // schema
                    JsonObject tunnelTableSchema = tree.get("schema").getAsJsonObject();
                    schema = new TunnelTableSchema(tunnelTableSchema);
                } else {
                    throw new TunnelException(requestId, "Incomplete session info: '" + json + "'");
                }
            }

            if (tree.has("slots") && tree.has("status")) {
                String status = tree.get("status").getAsString();
                if (status.equals("init")) {
                    throw new TunnelException(requestId, "Session is initiating. Session name: " + id);
                }
                // slots
                slots = new Slots(tree.getAsJsonArray("slots"), reload);
            } else {
                throw new TunnelException(requestId, "Incomplete session info: '" + json + "'");
            }

        } catch (TunnelException e) {
            throw e;
        } catch (Exception e) {
            throw new TunnelException(requestId, "Invalid json content: '" + json + "'", e);
        }
    }

    public void reloadSlots(Slot slot, String server, int slotNum) throws TunnelException {
        if (slots.getSlotNum() != slotNum) {
            // reload slot routes if slot num changed
            reload(slot.getServer());
        } else {
            // reset routed server slot rescheduled
            if (!slot.getServer().equals(server)) {
                slot.setServer(server, false);
            }
        }
    }

    public class Slot {
        private String slot;
        private String ip;
        private int port;

        public Slot(String slot, String server, boolean reload) throws TunnelException {
            if (slot.isEmpty() || server.isEmpty()) {
                throw new TunnelException("Slot or Routed server is empty");
            }
            this.slot = slot;
            // check empty server ip on init
            // fallback to old server ip on reload
            setServer(server, !reload);
        }

        public String getSlot() {
            return slot;
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public String getServer() {
            return ip + ":" + port;
        }

        public boolean equals(Slot slot) {
            return this.slot == slot.slot &&
                    this.ip == slot.ip &&
                    this.port == slot.port;
        }

        public void setServer(String server, boolean checkEmptyIp) throws TunnelException {
            String [] segs = server.split(":");
            if (segs.length != 2) {
                throw new TunnelException("Invalid slot format: " + server);
            }
            if (segs[0].isEmpty()) {
                if (checkEmptyIp) {
                    throw new TunnelException("Empty server ip: " + server);
                }
            } else {
                this.ip = segs[0];
            }
            this.port = Integer.valueOf(segs[1]);
        }
    }

    private class Slots implements Iterable<Slot> {
        private Random rand = new Random();
        private final List<Slot> slots;
        private int curSlotIndex;
        private Iterator<Slot> iter;

        public Slots(JsonArray slotElements, boolean reload) throws TunnelException {
            this.slots = new ArrayList<>();
            curSlotIndex = -1;

            for (JsonElement slot : slotElements) {

                if (!slot.isJsonArray()) {
                    throw new TunnelException("Invalid slot routes");
                }

                JsonArray slotInfo = slot.getAsJsonArray();
                if (slotInfo.size() != 2) {
                    throw new TunnelException("Invalid slot routes");
                }
                this.slots.add(new Slot(slotInfo.get(0).getAsString(), slotInfo.get(1).getAsString(), reload));
            }

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

        if (this.partitionSpec != null && this.partitionSpec.length() > 0) {
            params.put(TunnelConstants.RES_PARTITION, partitionSpec);
        }

        if (reocrdCount > 0) {
            params.put(TunnelConstants.RECORD_COUNT, String.valueOf(reocrdCount));
        }

        if (columns != null && columns.size() != 0) {
            params.put(TunnelConstants.ZORDER_COLUMNS, getColumnString());
        }

        HashMap<String, String> headers = new HashMap<String, String>();

        if (size < 0) {
            headers.put(Headers.TRANSFER_ENCODING, Headers.CHUNKED);
        } else {
            headers.put(Headers.CONTENT_LENGTH, String.valueOf(size));
        }

        headers.put(Headers.CONTENT_TYPE, "application/octet-stream");

        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));

        headers.put(HttpHeaders.HEADER_ODPS_SLOT_NUM, String.valueOf(slots.getSlotNum()));

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
            default: {
                throw new TunnelException("unsupported compression option.");
            }
        }

        headers.put(HttpHeaders.HEADER_ODPS_ROUTED_SERVER, slot.getServer());

        if (p2pMode) {
            try {
                URI u = new URI(tunnelServiceClient.getEndpoint());
                return tunnelServiceClient.connect(getResource(), "PUT", params, headers,
                        u.getScheme() + "://" + slot.getIp());
            } catch (URISyntaxException e) {
                throw new TunnelException("Invalid endpoint: " + tunnelServiceClient.getEndpoint());
            }
        } else {
            return tunnelServiceClient.connect(getResource(), "PUT", params, headers);
        }
    }

    private String getResource() {
        return conf.getResource(projectName, tableName)+ "/" + TunnelConstants.STREAMS;
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
        Connection conn = null;
        try {
            Slot slot = slots.iterator().next();
            conn = getConnection(pack.getCompressOption(), slot, pack.getTotalBytes(), pack.getSize());
            return sendBlock(pack, conn, slot, timeout);
        } catch (OdpsException e) {
            throw new IOException(e.getMessage(), e);
        } finally {
            if (null != conn) {
                conn.disconnect();
            }
        }
    }

    private String sendBlock(ProtobufRecordPack pack, Connection conn, Slot slot, long timeout)
            throws IOException, TunnelException {
        if (null == conn) {
            throw new IOException("Invalid connection");
        }
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
            TunnelException exception =
                    new TunnelException(response.getHeader(HEADER_ODPS_REQUEST_ID), conn.getInputStream(),
                            response.getStatus());
            throw new IOException(exception.getMessage(), exception);
        }

        reloadSlots(slot,
                    response.getHeader(HttpHeaders.HEADER_ODPS_ROUTED_SERVER),
                    Integer.valueOf(response.getHeader(HttpHeaders.HEADER_ODPS_SLOT_NUM)));

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
    public TableTunnel.StreamRecordPack newRecordPack() throws IOException {
        return new StreamRecordPackImpl(this, new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0));
    }

    @Override
    public TableTunnel.StreamRecordPack newRecordPack(CompressOption option) throws IOException {
        return new StreamRecordPackImpl(this, option);
    }

    @Override
    public Record newRecord() {
        return new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    }
}
