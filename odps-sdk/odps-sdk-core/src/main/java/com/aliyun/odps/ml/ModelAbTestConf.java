package com.aliyun.odps.ml;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "ABTest")
public class ModelAbTestConf{	
	@XmlElement(name = "Item")
	public List<ModelAbTestItem> items;
}