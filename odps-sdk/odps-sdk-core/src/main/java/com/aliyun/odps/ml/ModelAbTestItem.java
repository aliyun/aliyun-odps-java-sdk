package com.aliyun.odps.ml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Item")
public class ModelAbTestItem{
	@XmlElement(name = "Project")
	public String project;
	
	@XmlElement(name = "TargetModel")
	public String targetModel;
	
	@XmlElement(name = "Pct")
	public String Pct;
}