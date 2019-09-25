package com.aliyun.odps.ml;

import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import java.util.List;

@Root(name = "Pipeline", strict = false)
public class ModelPipeline {
	@ElementList(entry = "Processor", inline = true, required = false)
	public List<ModelProcessor> processors;
}
