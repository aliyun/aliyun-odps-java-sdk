package com.aliyun.odps.tunnel.io;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamWriter;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.StreamUploadSessionImpl;
import com.aliyun.odps.tunnel.TunnelException;

import java.io.IOException;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

/**
 * Time:    2019-12-08 10:07 AM
 * Author:  jingshan.mjs@alibaba-inc.com
 */
public class StreamRecordWriter extends ProtobufRecordStreamWriter {
    private Connection conn;
    private StreamUploadSessionImpl session;
    private StreamUploadSessionImpl.Slot slot;
    private boolean isClosed;
    private String traceId = null;

    /**
     * 构造此类对象
     *
     * @param schema {@link TableSchema}
     * @param conn   {@link Connection}
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public StreamRecordWriter(StreamUploadSessionImpl session, TableSchema schema, StreamUploadSessionImpl.Slot slot, Connection conn)
            throws IOException {
        this(session, schema, slot, conn, null);
    }

    public StreamRecordWriter(StreamUploadSessionImpl session, TableSchema schema, StreamUploadSessionImpl.Slot slot, Connection conn,
                              CompressOption option) throws IOException {

        super(schema, conn.getOutputStream(), option);
        this.session = session;
        this.slot = slot;
        this.conn = conn;
        this.isClosed = false;
    }

    @Override
    public void write(Record r) throws IOException {
        if (isClosed) {
            throw new IOException("Writer has been closed.");
        }

        try {
            super.write(r);
        } catch (IOException e) {
            Response resp = conn.getResponse();
            if (!resp.isOK()) {
                TunnelException err = new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID), conn.getInputStream(), resp.getStatus());
                throw new IOException(err.getMessage(), err);
            }
        }
    }

    @Override
    public void close() throws IOException {
        super.close();

        // handle response
        try {
            Response resp = conn.getResponse();
            if (!resp.isOK()) {
                TunnelException err = new TunnelException(resp.getHeader(HEADER_ODPS_REQUEST_ID), conn.getInputStream(), resp.getStatus());
                throw new IOException(err.getMessage(), err);
            }
            session.reloadSlots(slot,
                    resp.getHeader(HttpHeaders.HEADER_ODPS_ROUTED_SERVER),
                    Integer.valueOf(resp.getHeader(HttpHeaders.HEADER_ODPS_SLOT_NUM)));
            traceId = resp.getHeader(HEADER_ODPS_REQUEST_ID);
        } catch (TunnelException e) {
            throw new IOException(e.getMessage(), e);
        } finally {
            conn.disconnect();
            isClosed = true;
        }
    }

    public String getTraceId() {
        return traceId;
    }

}
