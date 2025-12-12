package com.aliyun.odps.udf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    // return an iterator of input splits in case of massive memory occupation
    // default using @planInputSplits for forward compatibility
    default Iterator<InputSplit> getIterableInputSplits(SplitStrategy strategy) throws IOException {
        return planInputSplits(strategy).iterator();
    }

    // spi definition could report splitting result stats
    // will be invoked after all InputSplit consumed
    //      optional stats keys:
    //          files.count: all files count of the returned InputSplits
    // default return an empty map for forward compatibility
    default Map<String, String> reportSplittingStats() throws IOException {
        return new HashMap<>();
    }

    void setRemainingPredicate(RowExpression expr);

    default void setLimit(long limit) {}
}