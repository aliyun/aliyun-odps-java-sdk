package com.aliyun.odps.ml;

import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;

@Root(name = "PredictDesc", strict = false)
public class ModelPredictDesc {
	@Element(name = "Pipeline", required = false)
	public ModelPipeline pipeline;

	@Element(name = "Target", required = false)
	public Target target;
}
