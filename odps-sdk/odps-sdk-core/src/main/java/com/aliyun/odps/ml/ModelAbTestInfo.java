package com.aliyun.odps.ml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "Onlinemodel")
public class ModelAbTestInfo{
	
	@XmlElement(name = "Project")
	public String project;
	
	@XmlElement(name = "Name")
	public String modelName;
	
	@XmlElement(name = "ABTest")
	public ModelAbTestConf abTestConf;
	
}