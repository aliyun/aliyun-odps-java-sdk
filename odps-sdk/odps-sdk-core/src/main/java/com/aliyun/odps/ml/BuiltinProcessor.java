package com.aliyun.odps.ml;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "BuiltinProcessor")
public class BuiltinProcessor extends AbstractProcessor {
	@XmlElement(name = "OfflinemodelProject")
	public String offlinemodelProject;
	
	@XmlElement(name = "OfflinemodelName")
	public String offlinemodelName;
}
