package com.aliyun.odps.udf;

public abstract class VectorizedStorageHandler extends OdpsStorageHandler {
    /**
     * @return Class providing an implementation of {@link InputSplitter}
     */
    public abstract Class<? extends InputSplitter> getInputSplitterClass();

    /**
     * Getter for underlying VectorizedExtractor class
     * @return Class description for the VectorizedExtractor class
     **/
    public abstract Class<? extends VectorizedExtractor> getVectorizedExtractorClass();

    /**
     * Getter for underlying VectorizedOutputer class
     * @return Class description for the VectorizedOutputer class
     **/
    public abstract Class<? extends VectorizedOutputer> getVectorizedOutputerClass();

    @Override
    public Class<? extends Extractor> getExtractorClass() {
        throw new RuntimeException("Not support for extractor by default");
    }

    @Override
    public Class<? extends Outputer> getOutputerClass() {
        throw new RuntimeException("Not support for outputer by default");
    }

    /**
     * @return null indicates plugin framework performs no commit action
     */
    public Class<? extends Committer> getCommitterClass() { return null; }
}