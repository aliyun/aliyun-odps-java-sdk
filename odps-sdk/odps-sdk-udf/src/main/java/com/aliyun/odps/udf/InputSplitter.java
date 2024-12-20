package com.aliyun.odps.udf;

import java.io.IOException;
import java.util.List;

public interface InputSplitter {
    class SplitStrategy {
        private long splitInBytes;

        public long getSplitInBytes() {
            return splitInBytes;
        }

        public void setSplitInBytes(long splitInBytes) {
            this.splitInBytes = splitInBytes;
        }
    }

    Class<? extends InputSplit> getInputSplitClass();

    void setup(List<String> locations, DataAttributes parameters) throws IOException;

    List<InputSplit> planInputSplits(SplitStrategy strategy) throws IOException;

    void setRemainingPredicate(RowExpression expr);

    default void setLimit(long limit) {}
}