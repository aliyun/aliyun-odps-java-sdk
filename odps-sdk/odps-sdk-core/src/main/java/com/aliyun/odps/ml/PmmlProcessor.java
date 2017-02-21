package com.aliyun.odps.ml;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "PmmlProcessor")
public class PmmlProcessor extends AbstractProcessor {
	@XmlElement(name = "Pmml")
	public String pmml;
	
	@XmlElement(name = "RefResource")
	public String refResource;
	
	@XmlElement(name = "RunMode")
	public String runMode;
}
