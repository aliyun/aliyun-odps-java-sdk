package com.aliyun.odps.udf;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.Closeable;
import java.io.IOException;

public abstract class VectorizedExtractor implements Closeable {
    public abstract void setup(ExecutionContext context, InputSplit inputSplit, DataAttributes parameters) throws IOException;

    public abstract void setRemainingPredicate(RowExpression expr);

    /**
     * Interface for read.
     * @param batchRows expect at-most batch rows.
     * @return actual read VectorSchemaRoot batch data.
     *      the service provider is responsible for constructing vectorSchemaRoot and fill data.
     * 		the service provider is responsible for memory management of vectorSchemaRoot.
     *      the service provider will try best allocate VSR vector with native memory (org.apache.arrow.memory.RootAllocator)
     *          to improve performance.
     *      the service provider should commit reserve native memory until next extract call or close call.
     *      return null to indicate the termination of data extracting.
     **/
    public abstract VectorSchemaRoot extract(int batchRows) throws IOException;

    public abstract long getReadBytes();

    /**
     * Interface for operations upon extractor exit, implementation can be no-op
     **/
    public abstract void close() throws IOException;
}
