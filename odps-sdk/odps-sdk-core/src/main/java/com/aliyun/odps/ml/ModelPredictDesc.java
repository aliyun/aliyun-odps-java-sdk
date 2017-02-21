package com.aliyun.odps.ml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "PredictDesc")
public class ModelPredictDesc {
	@XmlElement(name = "Pipeline")
	public ModelPipeline pipeline;

	@XmlElement(name = "Target")
	public Target target;
}
