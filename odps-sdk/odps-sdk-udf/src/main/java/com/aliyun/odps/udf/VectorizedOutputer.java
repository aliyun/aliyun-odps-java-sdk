package com.aliyun.odps.udf;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.io.Closeable;

public abstract class VectorizedOutputer implements Closeable {
    public abstract void setup(ExecutionContext context, DataAttributes parameters) throws IOException;

    /**
     * Interface for write
     * @param arrowBatch write data
     **/
    public abstract void output(VectorSchemaRoot arrowBatch) throws IOException;

    /**
     * @return null indicates no commit action will be performed by plugin framework.
     */
    public CommitMessage commit() { return null; }

    /**
     * Interface for operations upon outputer exit, implementation can be no-op
     **/
    public abstract void close() throws IOException;
}
