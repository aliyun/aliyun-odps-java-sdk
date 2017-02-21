package com.aliyun.odps.ml;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Pipeline")
public class ModelPipelineNew {
	@XmlElements({
        @XmlElement(name = "Processor", type = Processor.class),
        @XmlElement(name = "BuiltinProcessor", type = BuiltinProcessor.class),
        @XmlElement(name = "PmmlProcessor", type = PmmlProcessor.class)
    })
	public List<AbstractProcessor> processors;
}
