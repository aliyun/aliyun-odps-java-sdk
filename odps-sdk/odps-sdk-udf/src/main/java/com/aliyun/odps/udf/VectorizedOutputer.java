package com.aliyun.odps.udf;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

public abstract class VectorizedOutputer {
    public abstract void setup(ExecutionContext context, DataAttributes parameters) throws IOException;

    /**
     * Interface for write
     * @param arrowBatch write data
     * @return actual write data count. should equal to arrowBatch.getRowCount()
     **/
    public abstract int output(VectorSchemaRoot arrowBatch) throws IOException;

    /**
     * Interface for operations upon outputer exit, implementation can be no-op
     **/
    public abstract void close() throws IOException;
}
