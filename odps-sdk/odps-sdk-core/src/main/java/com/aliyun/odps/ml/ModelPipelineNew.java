package com.aliyun.odps.ml;

import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.ElementListUnion;
import com.aliyun.odps.simpleframework.xml.Root;
import java.util.List;


@Root(name = "Pipeline", strict = false)
public class ModelPipelineNew {
	@ElementListUnion({
        @ElementList(entry = "Processor", inline = true, type = Processor.class),
        @ElementList(entry = "BuiltinProcessor", inline = true, type = BuiltinProcessor.class),
        @ElementList(entry = "PmmlProcessor", inline = true, type = PmmlProcessor.class)
    })
	public List<AbstractProcessor> processors;
}
