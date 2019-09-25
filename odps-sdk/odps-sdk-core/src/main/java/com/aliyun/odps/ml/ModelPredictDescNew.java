package com.aliyun.odps.ml;

import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;

@Root(name = "PredictDesc", strict = false)
public class ModelPredictDescNew {
	@Element(name = "Pipeline", required = false)
	public ModelPipelineNew pipeline;

	@Element(name = "Target", required = false)
	public Target target;
}
