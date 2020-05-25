package com.aliyun.odps.ml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Offlinemodel")
public class OfflineModelInfo {
	@XmlElement(name = "Name")
	public String modelName;

	@XmlElement(name = "ModelPath")
	public String modelPath;

	@XmlElement(name = "Rolearn")
	public String rolearn;

	@XmlElement(name = "Type")
	public String type;

	@XmlElement(name = "Version")
	public String version;

	@XmlElement(name = "Processor")
	public String processor;

	@XmlElement(name = "Configuration")
	public String configuration;
}
