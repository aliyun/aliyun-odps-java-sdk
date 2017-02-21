package com.aliyun.odps.ml;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Pipeline")
public class ModelPipeline {
	@XmlElement(name = "Processor")
	public List<ModelProcessor> processors;
}