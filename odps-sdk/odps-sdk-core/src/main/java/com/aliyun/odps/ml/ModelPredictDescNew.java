package com.aliyun.odps.ml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "PredictDesc")
public class ModelPredictDescNew {
	@XmlElement(name = "Pipeline")
	public ModelPipelineNew pipeline;

	@XmlElement(name = "Target")
	public Target target;
}
