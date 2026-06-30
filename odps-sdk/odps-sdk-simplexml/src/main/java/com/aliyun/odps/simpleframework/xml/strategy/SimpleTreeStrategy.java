package com.aliyun.odps.simpleframework.xml.strategy;

import com.aliyun.odps.simpleframework.xml.stream.NodeMap;
import java.util.Map;

public class SimpleTreeStrategy extends TreeStrategy {
    @Override
    public boolean write(Type type, Object value, NodeMap node, Map map) {
        return false;
    }
}
